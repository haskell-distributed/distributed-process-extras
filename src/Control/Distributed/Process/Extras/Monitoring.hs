{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE ScopedTypeVariables #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Extras.Monitoring
-- Copyright   :  (c) Tim Watson 2013 - 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a primitive node monitoring capability, implemented as
-- a /distributed-process Management Agent/. Once the 'nodeMonitor' agent is
-- started, calling 'monitorNodes' will ensure that whenever the local node
-- detects a new network-transport connection (from another cloud haskell node),
-- the caller will receive a 'NodeUp' message in its mailbox. If a node
-- disconnects, a corollary 'NodeDown' message will be delivered as well.
--
-- When the client process first registers, events will be sent for each
-- connection the monitor process currently knows about.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Extras.Monitoring
  (
    NodeStatus(..)
  , nodeMonitorAgentId
  , nodeMonitorServiceName
  , nodeMonitor
  , monitorNodes
  , unmonitorNodes
  , nodes
  , Register(..)
  , UnRegister(..)
  ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process hiding (NodeId)
import Control.Distributed.Process.Internal.Types (NodeId(..))
import Control.Distributed.Process.Management
  ( MxEvent(MxConnected, MxDisconnected, MxNodeDied)
  , MxAgentId(..)
  , mxAgent
  , mxSink
  , mxReady
  , liftMX
  , mxGetLocal
  , mxSetLocal
  , mxNotify
  )
import Control.Distributed.Process.Extras (deliver)
import Data.Accessor
  ( Accessor
  , accessor
  , (^:)
  , (^.)
  )
import Data.Binary
import qualified Data.Foldable as Foldable
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set

import Data.Typeable (Typeable)
import GHC.Generics

import Network.Transport (EndPointAddress)

newtype Register = Register ProcessId
  deriving (Typeable, Generic)
instance Binary Register where
instance NFData Register where

newtype UnRegister = UnRegister ProcessId
  deriving (Typeable, Generic)
instance Binary UnRegister where
instance NFData UnRegister where

-- | Sent to subscribing processes when a connection or
-- disconnection (from a remote node) is detected.
--
data NodeStatus = NodeUp !NodeId | NodeDown !NodeId
  deriving (Typeable, Generic, Show)
instance Binary NodeStatus where
instance NFData NodeStatus where

data State = State { _clients :: HashSet ProcessId
                   , _peers   :: HashSet EndPointAddress
                   }

clients :: Accessor State (HashSet ProcessId)
clients = accessor _clients (\act' st -> st { _clients = act' })

peers :: Accessor State (HashSet EndPointAddress)
peers = accessor _peers (\act' st -> st { _peers = act' })

nodeMonitorServiceName :: String
nodeMonitorServiceName = "service.monitoring.nodes"

-- | The @MxAgentId@ for the node monitoring agent.
nodeMonitorAgentId :: MxAgentId
nodeMonitorAgentId = MxAgentId nodeMonitorServiceName

-- | Start monitoring node connection/disconnection events. When a
-- connection event occurs, the calling process will receive a message
-- @NodeUp NodeId@ in its mailbox. When a disconnect occurs, the
-- corollary @NodeDown NodeId@ message will be delivered instead.
--
-- No guaranatee is made about the timeliness of the delivery, nor can
-- the receiver expect that the node (for which it is being notified)
-- is still up/connected or down/disconnected at the point when it receives
-- a message from the node monitoring agent.
--
monitorNodes :: Process ()
monitorNodes = mxNotify . Register =<< getSelfPid

-- | Stop monitoring node connection/disconnection events. This does not
-- flush the caller's mailbox, nor does it guarantee that any/all node
-- up/down notifications will have been delivered before it is evaluated.
--
unmonitorNodes :: Process ()
unmonitorNodes = mxNotify . UnRegister =<< getSelfPid

nodes :: Process [NodeId]
nodes = do
  (sp, rp) <- newChan
  nsend nodeMonitorServiceName sp
  receiveChan rp

-- | Starts the node monitoring agent. No call to @monitorNodes@ and
-- @unmonitorNodes@ will have any effect unless the agent is already
-- running. Note that we make /no guarantees what-so-ever/ about the
-- timeliness or ordering semantics of node monitoring notifications.
--
nodeMonitor :: Process ProcessId
nodeMonitor =
  mxAgent nodeMonitorAgentId initState [
        (mxSink $ \(Register pid) -> do
            st <- mxGetLocal
            -- we want to ensure we notify newly registered clients of
            -- peers we already know are up...
            Foldable.mapM_ (liftMX . send pid . NodeUp . NodeId) $ (^. peers) st
            mxSetLocal $ (clients ^: Set.insert pid) st
            mxReady)
      , (mxSink $ \(UnRegister pid) -> do
            st <- mxGetLocal
            mxSetLocal $ (clients ^: Set.delete pid) st
            mxReady)
      , (mxSink $ \(sp :: (SendPort [NodeId])) -> do
            st <- mxGetLocal
            liftMX $ sendChan sp $ Set.toList $ Set.map NodeId (st ^. peers)
            mxReady)
      , (mxSink $ \ev -> maybeNotify ev >> mxReady)
    ]
  where
    initState :: State
    initState = State { _clients = Set.empty, _peers = Set.empty }

    maybeNotify (MxConnected    _   ep) = notify (nodeUp ep) >> store ep
    maybeNotify (MxDisconnected _   ep) = notify $ nodeDown ep
    maybeNotify (MxNodeDied     nid _ ) = notify $ nodeDown $ nodeAddress nid
    maybeNotify _                       = return ()
    -- when network errors cause disconnection, we don't always get
    -- a nice disconnected event..

    store ep = do
      st <- mxGetLocal
      mxSetLocal $ (peers ^: Set.insert ep) st

    notify msg = Foldable.mapM_ (liftMX . deliver msg) . _clients =<< mxGetLocal

    nodeUp   = NodeUp . NodeId
    nodeDown = NodeDown . NodeId
