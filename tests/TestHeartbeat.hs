{-# LANGUAGE ScopedTypeVariables   #-}
module Main where

import Control.Concurrent (threadDelay)
import Control.Distributed.Process.SysTest.Utils
import Control.Distributed.Process hiding (NodeId)
import Control.Distributed.Process.Internal.Types (NodeId(..), LocalNode(..))
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable()
import Control.Distributed.Process.Extras hiding (__remoteTable, monitor, send)
import qualified Control.Distributed.Process.Extras (__remoteTable)
import Control.Distributed.Process.Extras.Heartbeat
import Control.Distributed.Process.Extras.Monitoring
import Control.Distributed.Process.Debug
  ( traceLog
  , enableTrace
  , systemLoggerTracer
  , startTraceRelay
  )
import Control.Distributed.Process.Extras.SystemLog
import Control.Distributed.Process.Extras.Time
import Control.Exception  (IOException)
import qualified Control.Exception as Ex (try)
import Control.Monad (void)
#if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
#endif

import Test.Framework (Test, testGroup, defaultMain)
import Test.Framework.Providers.HUnit (testCase)
import Network.Socket (sClose)
import qualified Network.Transport as NT (Transport)
import Network.Transport.TCP
  ( createTransportExposeInternals
  , TransportInternals(socketBetween)
  , TCPParameters(transportConnectTimeout, tcpKeepAlive)
  , defaultTCPParameters
  )

testMonitorNodeDeath :: NT.Transport
                     -> TransportInternals
                     -> TestResult ()
                     -> Process ()
testMonitorNodeDeath transport internals result = do
    void $ nodeMonitor >> monitorNodes   -- start node monitoring

    n1 <- getSelfNode

    node2 <- liftIO $ do
      n <- newLocalNode transport initRemoteTable
      forkProcess n (startHeartbeatService (seconds 5) >> return ())
      return n

    let n2 = localNodeId node2

    -- let's have some tracing to help diagnose problems

    systemLog (const say) (return ()) Debug return

    -- enableTrace =<< spawnLocal systemLoggerTracer
    -- _ <- startTraceRelay n2
    -- liftIO $ threadDelay 1000

    info logChannel "node2 running"

    -- sending to (nodeId, "ignored") is a short cut to force a connection
    _ <- liftIO $ tryForkProcess node2 $ ensureNodeRunning (n1, "ignored")

    traceLog "node2 connected"

    -- we've seen node2 come online and connect to us...
    (NodeUp _) <- receiveWait [ matchIf (\(NodeUp n2') -> n2' == n2)
                                        return ]

    traceLog "seen node2 up - breaking connection"

    breakConnection internals n1 n2

    traceLog "node 2 disconnected"

    -- NB: it would be 'nice' if we could simulate the network
    -- silently failing and network-transport not noticing, but
    -- I've not figured out how to do that simply in this case,
    -- so we will verify connectivity here in some other way

    (NodeDown _) <- expect

    traceLog "seen node 2 down"
    -- liftIO $ closeLocalNode node2

    traceLog "starting local heartbeat process"

    _ <- startHeartbeatService $ seconds 5

    traceLog "waiting for NodeUp (due to heartbeats)"

    -- since we're now heartbeating, we should see the node down
    Just (NodeUp _) <- receiveTimeout
                                (after 10 Seconds)
                                [ matchIf (\(NodeUp n2') -> n2' == n2)
                                          return ]
    traceLog "done!"
    stash result ()

    where
      ensureNodeRunning nid = sendTo nid "connected"

      breakConnection :: TransportInternals
                      -> NodeId
                      -> NodeId
                      -> Process ()
      breakConnection internals' addr1 addr2 = do
          sock <- liftIO $ Ex.try $ socketBetween internals'
                                                  (nodeAddress addr1)
                                                  (nodeAddress addr2)
          -- either (\e -> const (return ()) (e :: IOException))
          case sock of
            (Right eSock)              -> liftIO $ sClose eSock
            (Left  (e :: IOException)) -> traceLog $ "breakConnection: " ++ (show e)

myRemoteTable :: RemoteTable
myRemoteTable = Control.Distributed.Process.Extras.__remoteTable initRemoteTable

--------------------------------------------------------------------------------
-- Utilities and Plumbing                                                     --
--------------------------------------------------------------------------------

tests :: NT.Transport -> TransportInternals -> LocalNode  -> [Test]
tests transport internals localNode = [
    testGroup "Node Monitoring" [
        testCase "Heartbeats"
          (delayedAssertion
           "tbc"
           localNode () (testMonitorNodeDeath transport internals))
      ]
  ]

primitivesTests :: NT.Transport -> TransportInternals -> IO [Test]
primitivesTests transport internals = do
  localNode <- newLocalNode transport initRemoteTable
  let testData = tests transport internals localNode
  return testData

-- | Given a @builder@ function, make and run a test suite on a single transport
testMain :: (NT.Transport -> TransportInternals -> IO [Test]) -> IO ()
testMain builder = do
  Right (transport, internals) <- createTransportExposeInternals
                                     "127.0.0.1" "0"
                                     defaultTCPParameters {
                                       tcpKeepAlive = False
                                     , transportConnectTimeout = Just 100000
                                     }
  testData <- builder transport internals
  defaultMain testData

main :: IO ()
main = testMain primitivesTests
