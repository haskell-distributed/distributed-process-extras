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
-- timing is vaugue, ordering is not guaranteed, best efforts...
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
import Control.Distributed.Process.Extras.SystemLog
  ( logChannel
  )
import qualified Control.Distributed.Process.Extras.SystemLog as Log
import Control.Distributed.Process.Extras.Time (TimeInterval, asTimeout)
import Control.Monad (foldM)
import Control.Monad.Reader (ask)
import Data.Binary
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
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
  sender (ti, sp)
  receiveChan rp

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
             | HBAgentBoot deriving (Show)

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
    handleConfig (t, c) = do
      st <- mxGetLocal
      liftMX $ Log.info logChannel $ serviceName ++ ".configure: " ++ show st
      configureAgent t st >> liftMX (sendChan c ()) >> mxReady

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
                           -- folks we know about
                         , tracked  :: Map NodeId Int
                           -- folks we've not heard from, and how many timeouts
                           -- since we last heard from them
                         } deriving (Show)

serverName :: String
serverName = "service.monitoring.heartbeat.server"

startHeartbeatServer :: TimeInterval -> [NodeId] -> Process ()
startHeartbeatServer ti elems = do
  Log.info logChannel $ serverName ++ ".configure { timeInterval= " ++ show ti ++
                                      ", initialPeerSet= " ++ show elems ++ " }"
  (us, here) <- self
  register serverName us
  serve $ HBServer here ti (Set.fromList elems)
                           (Map.fromList [(e, 0) | e <- elems])

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
                         , tracked = Map.insert (NodeId ep) 0 tracked
                         })
            -- we can see the death of a peer in two ways
          , match (\(MxDisconnected _ ep) -> cleanup hbs (NodeId ep))
          , match (\(MxNodeDied nid why) ->
              -- we leave the dead node in our list of peers for now, since
              -- we'll try to contact them 4 * timeout before giving up
              if why == DiedDisconnect
                then return hbs { tracked = Map.adjust (+1) nid tracked }
                else cleanup hbs nid
              )
          , match (\(MxSent origin _ _) ->
              -- TODO: enforce the state invariant that we /cannot/ see
              -- MxSent here if the origin node isn't in peers already
              return hbs { tracked = Map.delete (processNodeId origin) tracked })
--          , match (\(_ :: TimeInterval) -> die "foo") -- return $ (timeOut ^= ti) hbs)
          ]

  where

    cleanup ss@HBServer{ peers = p, tracked = tr} nid =
      return ss { peers = Set.delete nid p
                , tracked = Map.delete nid tr }

    -- if we timed out waiting then it's time to send a heartbeat
    maybeHeartbeat s Nothing   = tick s >>= serve
    maybeHeartbeat _ (Just s') = serve s'

    tick :: HBServer -> Process HBServer
    tick s@HBServer{ peers = ps, tracked = ts } = do
      -- let trPrs   = Set.fromList (Map.keys ts)
      --    diffPrs = [(p, 1) | p <- Set.toList $ Set.difference ps trPrs]
      --    ts'     = Map.union ts (Map.fromList diffPrs) in do
      Log.debug logChannel $ serverName ++ ".tick: " ++ show ts
        -- tr <- mapM (checkTick s) $ ts'

        -- our invariant is that no tracked node-id should be missing from
        -- the set of peers, and if a peer is not being tracked, we don't
        -- communicate with it this time around, but we do
      foldM checkTick s ps


    checkTick :: HBServer -> NodeId -> Process HBServer
    checkTick hs@HBServer{ home = h
                         , delay = d
                         , peers = p
                         , tracked = t } n =
      let (old, new)    = Map.updateLookupWithKey bumpCount n t in
      case old of
        (Just 4) -> do alarm n d
                       sendTick h n
                       return $ hs{ peers = Set.delete n p, tracked = new }
        _        -> do sendTick h n
                       return $ hs{ tracked = new }

    bumpCount _ count
      | count == 4 = Nothing
      | otherwise  = Just (count + 1)

    alarm n t = nsend serviceName (NodeUnresponsive n t)

    sendTick here nid = do
      us <- getSelfPid
      this <- processNode <$> ask
      Log.debug logChannel $ serverName ++ ".tick: " ++ show nid
      sendTo (nid, serviceName) here
      liftIO $ disconnect this (ProcessIdentifier us) (NodeIdentifier nid)
