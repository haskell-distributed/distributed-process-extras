{-# LANGUAGE TupleSections #-}
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
-- This module provides a heartbeat service that builds on the capability in
-- "Control.Distributed.Process.Extras.Monitoring". Once started, a heartbeat
-- service will register an interest in (dis-)connection events via the
-- @monitorNodes@ API, and keeps track of all connected network-transport
-- endpoints. Parameterised with a timeout setting, each time the delay
-- expires
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Extras.Heartbeat
  ( startHeartbeatServer
  , serviceName
  ) where

import Control.Applicative (liftA2)
import Control.Distributed.Process -- NB: requires NodeId(..) to be exported!
import Control.Distributed.Process.Internal.Types
  ( Identifier(NodeIdentifier, ProcessIdentifier)
  , LocalProcess(processId, processNode)
  )
import Control.Distributed.Process.Internal.Messaging (disconnect)
import Control.Distributed.Process.Extras (Routable(..))
import Control.Distributed.Process.Extras.Monitoring
  ( NodeStatus(NodeUp, NodeDown)
  , monitorNodes
  )
import Control.Distributed.Process.Extras.Time (TimeInterval, asTimeout)
import Control.Monad.Reader (asks)
import Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import Data.Foldable (mapM_)

type Tick = ()
netTick :: Tick
netTick = ()

serviceName :: String
serviceName = "service.monitoring.heartbeat"

startHeartbeatServer :: TimeInterval -> Process ()
startHeartbeatServer timeOut = do
  register serviceName =<< getSelfPid
  monitorNodes
  startServer timeOut

startServer :: TimeInterval -> Process ()
startServer = flip serve Set.empty

serve :: TimeInterval -> HashSet NodeId -> Process ()
serve t s =
  maybeHeartbeat t s =<<
    receiveTimeout
          (asTimeout t)
          [ match (\(NodeUp   nid) -> return $ Set.insert nid s)
          , match (\(NodeDown nid) -> return $ Set.delete nid s)
          , match (\()             -> return s)
          ]

  where

    -- if we timed out waiting then it's time to send a heartbeat
    maybeHeartbeat timeOut' st Nothing   = tick st >> serve timeOut' st
    maybeHeartbeat timeOut' _ (Just st') = serve timeOut' st'

    tick :: HashSet NodeId -> Process ()
    tick = mapM_ sendTick

    sendTick nid = do
      (us, node) <- asks (liftA2 (,) processId processNode)
      -- (us, them) <- liftA2 (,) processId processNode <$> ask
      sendTo (nid, serviceName) netTick
      liftIO $ disconnect node (ProcessIdentifier us) (NodeIdentifier nid)
