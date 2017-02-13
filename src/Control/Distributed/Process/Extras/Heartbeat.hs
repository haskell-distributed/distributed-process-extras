{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE PatternGuards       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE RecordWildCards     #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Extras.Heartbeat
-- Copyright   :  (c) Tim Watson 2017
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a heartbeat service, in a similar vein to the /heart/
-- program that ships with Erlang/OTP, and the net_ticktime configuration of
-- Erlang's net_kernel application.
--
-- [Important Caveats]
--
-- This service functions similarly to the aforementioned components in
-- Erlang/OTP, but has different semantics. Specifically, this sevice makes
-- no guarantees about the reliability, performance, or semantics of
-- inter-node connectivity in cloud haskell. It works on /best efforts/ and
-- deliberately employs a very simplistic approach.
--
-- This service /might/ be helpful in a production environment, but it also
-- /might not/, and in some situations, this service /could/ lead to problems
-- such as performance degredation and/or other network related issues.
--
-- __Use at your own risk.__
--
-- [Overview]
--
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Extras.Heartbeat
  ( startHeartbeatService
  -- , stopHeartbeatService
  , configureTimeout
  , serviceName
  , serverName
  , registerListener
  , unregisterListener
  , NodeUnresponsive(nodeId, tickTime)
  ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process -- NB: requires NodeId(..) to be exported!
import qualified Control.Distributed.Process.UnsafePrimitives as Unsafe
import Control.Distributed.Process.Internal.Types
  ( Identifier(NodeIdentifier, ProcessIdentifier)
  , LocalProcess(processNode)
  )
import Control.Distributed.Process.Internal.Messaging (disconnect)
import Control.Distributed.Process.Extras.Internal.Primitives
  ( self
  , spawnSignalled
  , deliver
  )
import Control.Distributed.Process.Extras.Internal.Types (Routable(..))
import Control.Distributed.Process.Extras.Timer
  ( periodically
  , TimerRef
  )
import Control.Distributed.Process.Management
  ( MxEvent(..)
  , MxAgentId(..)
  , MxAgent
  , MxAction
  , mxAgent
  , mxSink
  , mxReady
  , mxSkip
  , liftMX
  , mxGetLocal
  , mxSetLocal
  , mxUpdateLocal
  )
import qualified Control.Distributed.Process.Extras.Monitoring as NodeMon
import Control.Distributed.Process.Extras.Monitoring
  ( Register(..)
  , UnRegister(..)
  )
import Control.Distributed.Process.Extras.Time (TimeInterval, asTimeout)
import Control.Monad.Reader (ask)
import Data.Binary
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import Data.Traversable (mapM)
import Data.Foldable (mapM_)
import Data.Typeable (Typeable)
import GHC.Generics

{- [note: implementation guide]

The heartbeat service consists of three parts:

Each time we *see* a peer, we reset the warning timer

-}

data NodeUnresponsive =
  NodeUnresponsive { nodeId   :: NodeId
                   , tickTime :: TimeInterval }
    deriving (Typeable, Generic, Eq, Show)
instance Binary NodeUnresponsive where
instance NFData NodeUnresponsive where

serviceName :: String
serviceName = "service.monitoring.heartbeat.agent"

-- | The @MxAgentId@ for the node monitoring agent.
heartbeatAgentId :: MxAgentId
heartbeatAgentId = MxAgentId serviceName

configureTimeout :: TimeInterval -> Process ()
configureTimeout ti =
  doConfigureTimeout ti (nsend serviceName)

doConfigureTimeout :: TimeInterval
                   -> ((TimeInterval, SendPort ()) -> Process ())
                   ->  Process ()
doConfigureTimeout ti sender = do
  (sp, rp) <- newChan
  sender (ti, sp) >> receiveChan rp

-- | Blah
--
-- No guaranatee is made about the timeliness of the delivery, nor can
-- the receiver expect that the node (for which it is being notified)
-- is still up/connected or down/disconnected at the point when it receives
-- a message from the service (i.e., the connection state of the peer may
-- have changed by the time the message arrives).
--
registerListener :: Process ()
registerListener = nsend serviceName . Register =<< getSelfPid

-- | Stop listening for node unresponsive events. This does not
-- flush the caller's mailbox, nor does it guarantee that any/all node
-- unresponsive notifications will have been delivered before it is evaluated.
--
unregisterListener :: Process ()
unregisterListener = nsend serviceName . UnRegister =<< getSelfPid

-- Agent implementation

data HBAgent = HBAgent { node     :: NodeId
                       , hbDelay  :: TimeInterval
                       , hbServer :: ProcessId
                       , clients  :: HashSet ProcessId
                       }
             | HBAgentBoot

-- | Starts the heartbeat service
startHeartbeatService :: TimeInterval -> Process ProcessId
startHeartbeatService ti = do
  pid <- mxAgent heartbeatAgentId
                 HBAgentBoot
                 [ mxSink handleConfig
                 , mxSink handleMxEvent
                 , mxSink (\(ev :: NodeUnresponsive) -> do
                      mapM_ (liftMX . deliver ev) . clients =<< mxGetLocal
                      mxReady)
                 , mxSink (\(Register pid) -> updateClients (Set.insert pid))
                 , mxSink (\(UnRegister pid) -> updateClients (Set.delete pid))
                 ]
  doConfigureTimeout ti (send pid :: (TimeInterval, SendPort()) -> Process ())
  return pid

  where

    updateClients op = do
      mxUpdateLocal $ \st@HBAgent{ clients = c } -> st { clients = op c }
      mxReady

    handleConfig :: (TimeInterval, SendPort ()) -> MxAgent HBAgent MxAction
    handleConfig (t, c) =
      mxGetLocal >>= configureAgent t >> liftMX (sendChan c ()) >> mxReady

    configureAgent curTi st
      | HBAgentBoot <- st = do
          (us, sv) <- liftMX $ do
            us' <- getSelfNode
            sv' <- spawnSignalled NodeMon.nodes (startHeartbeatServer curTi)
            return (us', sv')
          mxSetLocal HBAgent{ node     = us
                            , hbDelay  = curTi
                            , hbServer = sv
                            , clients  = Set.empty
                            }
      | HBAgent{ hbServer = pid } <- st = liftMX $ send pid curTi

    handleMxEvent :: MxEvent -> MxAgent HBAgent MxAction
    handleMxEvent ev = mxGetLocal >>= checkEv ev

    -- passing the state here is unnecessary but convenient
    checkEv :: MxEvent -> HBAgent -> MxAgent HBAgent MxAction
    checkEv _  HBAgentBoot              = mxSkip
    checkEv ev hbs@HBAgent{ node = n }
        | (MxNodeDied nid why)   <- ev
        , nid /= n              -- do we really need to? It can't be us!
        , why `elem` [ DiedNormal
                     , DiedUnknownId
                     , DiedDisconnect ] = forwardToServer ev hbs
        | (MxProcessDied who _)  <- ev  = handleRestarts who hbs
        | MxConnected{}          <- ev  = forwardToServer ev hbs
        | MxDisconnected{}       <- ev  = forwardToServer ev hbs
        | MxSent{}               <- ev  = maybeForwardToServer ev =<< mxGetLocal
        | otherwise                     = mxSkip

    maybeForwardToServer e@(MxSent origin _ _) st@HBAgent{node = n}
        | to' <- processNodeId origin
        , to' /= n           = forwardToServer e st
        | otherwise          = mxReady
    maybeForwardToServer _ _ = mxReady

    -- it's gross that we need to think about Boot here, instead of only
    -- in the handlers that actually deal with booting up - a dual State
    -- interface would be a lot nicer - perhaps Agent s1 s2 MxAction ??

    forwardToServer _   HBAgentBoot = liftMX terminate  -- state invariant
    forwardToServer ev' HBAgent{ hbServer = sPid }
                                    = liftMX (send sPid ev') >> mxReady

    handleRestarts _   HBAgentBoot = liftMX terminate   -- state invariant
    handleRestarts pid sta@HBAgent{ hbServer = sPid' }
        | pid == sPid' = restartBoth sta
        | otherwise    = mxReady

    restartBoth HBAgentBoot            = liftMX terminate -- state invariant
    restartBoth HBAgent{ hbDelay = d } =
      configureAgent d HBAgentBoot >> mxReady

data HBServer = HBServer { home     :: NodeId
                         , delay    :: TimeInterval
                         , peers    :: HashSet NodeId
                         , tracked  :: HashMap NodeId Int
                         , timerRef :: TimerRef
                         }

serverName :: String
serverName = "service.monitoring.heartbeat.server"

startHeartbeatServer :: TimeInterval -> [NodeId] -> Process ()
startHeartbeatServer ti elems = do
  (us, here) <- self
  register serverName us
  tm <- startTimeoutManager ti us
  serve $ HBServer here ti (Set.fromList elems) Map.empty tm

startTimeoutManager :: TimeInterval -> ProcessId -> Process TimerRef
startTimeoutManager ti pid =
  periodically ti (Unsafe.send pid ())

-- new connections are relevant, as are disconnections
  -- disconnect == , so don't track them any more

-- MxSent from _ -> if from is remote, reset the timer
-- MxNodeDied _ reason ->
  -- if reason is DiedUnknownId, remove from peers
  -- if reason is DiedNormal, remove from peers
  -- if the reason is DiedDisconnect then we *do* want to keep in contact
-- all other messages are filtered out

serve :: HBServer -> Process ()
serve hbs@HBServer{..} =
  maybeHeartbeat hbs =<<
    receiveTimeout
          (asTimeout delay)
          [ match (\(MxConnected _ ep) ->
              return hbs { peers = Set.insert (NodeId ep) peers
                         , tracked = Map.insert (NodeId ep) 1 tracked
                         })
            -- we can see the death of a peer in two ways
          , match (\(MxDisconnected _ ep) -> cleanup hbs (NodeId ep))
          , match (\(MxNodeDied nid _) -> cleanup hbs nid)
          , match (\(MxSent origin _ _) ->
              -- TODO: enforce the state invariant that we /cannot/ see
              -- MxSent here if the origin node isn't in peers already
              return hbs { tracked = Map.delete (processNodeId origin) tracked })
          , match (\(_ :: TimeInterval) -> die "foo") -- return $ (timeOut ^= ti) hbs)
          ]

  where

    cleanup ss@HBServer{ peers = p, tracked = tr} nid =
      return ss { peers = Set.delete nid p
                , tracked = Map.delete nid tr }

    -- if we timed out waiting then it's time to send a heartbeat
    maybeHeartbeat s Nothing   = tick s >>= serve
    maybeHeartbeat _ (Just s') = serve s'

    tick :: HBServer -> Process HBServer
    tick s@HBServer{ tracked = trPids } = do
      tr <- mapM (checkTick s) $ Map.toList trPids
      return $ s { tracked = Map.fromList tr }

    checkTick HBServer{ home = h, delay = d } st@(there, count)
      | count == 4 = alarm there d >> sendTick h there >> return st
      | otherwise  = sendTick h there >> return (there, count + 1)

    alarm n t = nsend serviceName (NodeUnresponsive n t)

    sendTick here nid = do
      us <- getSelfPid
      this <- processNode <$> ask
      sendTo (nid, serviceName) here
      liftIO $ disconnect this (ProcessIdentifier us) (NodeIdentifier nid)
