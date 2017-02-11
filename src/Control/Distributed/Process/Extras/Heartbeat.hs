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
  , serviceName
  , serverName
  , NodeUnresponsive(..)
  ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process -- NB: requires NodeId(..) to be exported!
import qualified Control.Distributed.Process.UnsafePrimitives as Unsafe
import Control.Distributed.Process.Internal.Types
  ( Identifier(NodeIdentifier, ProcessIdentifier)
  , LocalProcess(processNode)
  )
import Control.Distributed.Process.Internal.Messaging (disconnect)
import Control.Distributed.Process.Extras
  ( Routable(..)
  , spawnSignalled
  )
import Control.Distributed.Process.Extras.Internal.Primitives
  ( self
  )
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
  , mxNotify
  )
import qualified Control.Distributed.Process.Extras.Monitoring as NodeMon
import Control.Distributed.Process.Extras.Time (TimeInterval, asTimeout)
import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (^.)
  , (^=)
  )
import Control.Monad.Reader (ask)
import Data.Binary
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
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
configureTimeout ti = do
  doConfigureTimeout ti (nsend serviceName)

doConfigureTimeout :: TimeInterval
                   -> ((TimeInterval, SendPort ()) -> Process ())
                   ->  Process ()
doConfigureTimeout ti sender = do
  (sp, rp) <- newChan
  sender (ti, sp) >> receiveChan rp

-- Agent implementation

data HBAgent = HBAgent { _node     :: NodeId
                       , _hbDelay  :: TimeInterval
                       , _hbServer :: ProcessId
                       , _clients  :: HashSet ProcessId
                       }
             | HBAgentBoot

node :: Accessor HBAgent NodeId
node = accessor _node (\act' st -> st { _node = act' })

hbDelay :: Accessor HBAgent TimeInterval
hbDelay = accessor _hbDelay (\act' st -> st { _hbDelay = act' })

hbServer :: Accessor HBAgent ProcessId
hbServer = accessor _hbServer (\act' st -> st { _hbServer = act' })

clients :: Accessor HBAgent (HashSet ProcessId)
clients = accessor _clients (\act' st -> st { _clients = act' })

-- | Starts the heartbeat service
startHeartbeatService :: TimeInterval -> Process ProcessId
startHeartbeatService ti = do
  pid <- mxAgent heartbeatAgentId
                 HBAgentBoot
                 [ mxSink handleConfig
                 , mxSink handleMxEvent ]
  doConfigureTimeout ti (send pid :: (TimeInterval, SendPort()) -> Process ())
  return pid

  where

    handleConfig :: (TimeInterval, SendPort ()) -> MxAgent HBAgent MxAction
    handleConfig (t, c) =
      mxGetLocal >>= configureAgent t >> (liftMX $ sendChan c ()) >> mxReady

    configureAgent curTi st
      | HBAgentBoot <- st = do
          (us, sv) <- liftMX $ do
            us' <- getSelfNode
            sv' <- spawnSignalled NodeMon.nodes (startHeartbeatServer curTi)
            return (us', sv')
          mxSetLocal $ ( (node     ^= us)
                       . (hbDelay  ^= curTi)
                       . (hbServer ^= sv)
                       $ HBAgent{} )
      | otherwise = liftMX $ send (st ^. hbServer) curTi

    handleMxEvent :: MxEvent -> MxAgent HBAgent MxAction
    handleMxEvent ev = mxGetLocal >>= checkEv ev

    -- passing the state here is unnecessary but convenient
    checkEv :: MxEvent -> HBAgent -> MxAgent HBAgent MxAction
    checkEv ev hbs
        | (MxNodeDied nid why)   <- ev
        , nid /= (hbs ^. node)  -- do we really need to? It can't be us!
        , why `elem` [ DiedNormal
                     , DiedUnknownId
                     , DiedDisconnect] = forwardToServer ev hbs
        | (MxProcessDied who _)  <- ev = handleRestarts who hbs
        | MxConnected{}          <- ev = forwardToServer ev hbs
        | MxDisconnected{}       <- ev = forwardToServer ev hbs
        | MxSent{}               <- ev = maybeForwardToServer ev =<< mxGetLocal
        | otherwise                    = mxSkip

    maybeForwardToServer e@(MxSent origin _ _) st
        | to' <- processNodeId origin
        , to' /= (st ^. node) = forwardToServer e st
        | otherwise          = mxReady
    maybeForwardToServer _ _ = mxReady

    -- it's gross that we need to think about Boot here, instead of only
    -- in the handlers that actually deal with booting up - a dual State
    -- interface would be a lot nicer - perhaps Agent s1 s2 MxAction ??
    forwardToServer ev' hbs'
        | HBAgentBoot <- hbs' = mxReady  -- do nothing while we're booting
        | otherwise           = liftMX (send (hbs' ^. hbServer) ev') >> mxReady

    handleRestarts pid sta
        | pid == (sta ^. hbServer) = restartBoth sta
        | otherwise                = mxReady

    restartBoth s =
      configureAgent (s ^. hbDelay) HBAgentBoot >> mxReady

data Signal = Signal NodeId
  deriving (Typeable, Generic, Eq, Show)
instance Binary Signal where

data HBServer = HBServer { _home     :: NodeId
                         , _peers    :: HashSet NodeId
                         , _unknowns :: HashSet NodeId
                         , _delay    :: TimeInterval
                         , _timerRef :: TimerRef
                         }

peers :: Accessor HBServer (HashSet NodeId)
peers = accessor _peers (\act' st -> st { _peers = act' })

unknowns :: Accessor HBServer (HashSet NodeId)
unknowns = accessor _unknowns (\act' st -> st { _unknowns = act' })

home :: Accessor HBServer NodeId
home = accessor _home (\act' st -> st { _home = act' })

timeOut :: Accessor HBServer TimeInterval
timeOut = accessor _delay (\act' st -> st { _delay = act' })

timerRef :: Accessor HBServer TimerRef
timerRef = accessor _timerRef (\act' st -> st { _timerRef = act' })

serverName :: String
serverName = "service.monitoring.heartbeat.server"

startHeartbeatServer :: TimeInterval -> [NodeId] -> Process ()
startHeartbeatServer ti elems = do
  (us, here) <- self
  register serverName us
  tm <- startTimeoutManager ti us
  serve $ HBServer here (Set.fromList elems) Set.empty ti tm

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
serve hbs =
  maybeHeartbeat hbs =<<
    receiveTimeout
          (asTimeout $ hbs ^. timeOut)
          [ match (\(MxConnected    _   ep) ->
              return $ update Set.insert (NodeId ep) hbs) -- tracking new peer
          , match (\(MxDisconnected _   ep) -> -- valid removal of peer
              return $ update Set.delete (NodeId ep) hbs)
          , match (\(MxNodeDied     nid _)  ->
              return $ update Set.delete nid hbs)
          , match (\(MxSent origin  _   _)  ->
              return $ (unknowns ^: Set.delete (processNodeId origin)) hbs)
          , match (\(_ :: TimeInterval)     -> die "foo") -- return $ (timeOut ^= ti) hbs)
          ]

  where

    update op ep' hbs' = ((peers ^: op ep') . (unknowns ^: op ep')) hbs'

    -- if we timed out waiting then it's time to send a heartbeat
    maybeHeartbeat s Nothing   = tick s >> serve s
    maybeHeartbeat _ (Just s') = serve s'

    tick :: HBServer -> Process ()
    tick HBServer{..} = mapM_ (sendTick _home) _unknowns

    sendTick here nid = do
      us <- getSelfPid
      this <- processNode <$> ask
      sendTo (nid, serviceName) here
      liftIO $ disconnect this (ProcessIdentifier us) (NodeIdentifier nid)

--------------------------------------------------------------------------------
-- Timer Implementation                                                       --
--------------------------------------------------------------------------------

{-
startTimer :: Delay -> Process TimeoutSpec
startTimer d
  | Delay t <- d = do sig <- liftIO $ newEmptyTMVarIO
                      tref <- runAfter t $ liftIO $ atomically $ putTMVar sig ()
                      return (d, Just (tref, (readTMVar sig)))
  | otherwise    = return (d, Nothing)


-- runs the timer process
runTimer :: TimeInterval -> Process () -> Bool -> Process ()
runTimer t proc cancelOnReset = do
    cancel <- expectTimeout (asTimeout t)
    -- say $ "cancel = " ++ (show cancel) ++ "\n"
    case cancel of
        Nothing     -> runProc cancelOnReset
        Just Cancel -> return ()
        Just Reset  -> if cancelOnReset then return ()
                                        else runTimer t proc cancelOnReset
  where runProc True  = proc
        runProc False = proc >> runTimer t proc cancelOnReset
-}
