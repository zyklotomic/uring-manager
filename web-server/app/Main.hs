{-# LANGUAGE OverloadedStrings #-}
module Main where 

import Control.Concurrent (forkIO, forkOn, getNumCapabilities, yield)
import Control.Monad (forever, forM_, forM)
import qualified Data.ByteString as B
import Data.ByteString.Char8 (ByteString, pack)
import Network.Socket (Socket, SocketType(..), AddrInfoFlag(..),
                       SocketOption(..), accept, addrAddress, addrFamily,
                       addrFlags, bind, defaultProtocol, defaultHints, withFdSocket,
                       fdSocket, getAddrInfo, listen, setSocketOption, socket, close, Family(..), AddrInfo(..))
import qualified Network.Socket.ByteString as SB (sendAll, recv)
import System.Environment (getArgs)
import System.Mem (performGC)
 
main :: IO ()
main = mainPerCapability

mainPerCapability :: IO ()
mainPerCapability = do
  listenSock <- startListenSock
  n <- getNumCapabilities
  putStrLn $ "starting percap! ncap: " ++ show n
  forM_ [0..n-1] $ \i ->
    forkOn i $ acceptLoop i listenSock
  acceptLoop n listenSock
  where
    acceptLoop i listenSk = forever $ do
      (sock,_) <- accept listenSk
      forkOn i $ worker sock  

mainOriginal :: IO ()
mainOriginal = do
  listenSock <- startListenSock
  n <- getNumCapabilities
  putStrLn $ "starting original! ncap: " ++ show n
  -- putStrLn $ "uring manager: " ++ show UM.supportsIOURing
  -- forM_ listenSocks $ \listenSock -> forkIO $
  forever $ do
    (sock, _) <- accept listenSock
    -- putStrLn "accepted!"
    forkIO $ worker sock 

startListenSock :: IO Socket
startListenSock = do
  args <- getArgs
  let portNumber = head args
  addrinfos <- getAddrInfo
                 (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                 (Just "169.254.1.200")
                 -- Nothing
                 (Just $ portNumber)
  let serveraddr = head addrinfos
  -- let cond = \addrInfo -> (addrFamily addrInfo == AF_INET6) && (addrSocketType addrInfo == Stream)
  --     serveraddr = head . filter cond $ addrinfos
  -- forM addrinfos $ \serveraddr -> do
  putStrLn $ "listening on addr: " ++ show serveraddr
  -- listenSock <- socket (addrFamily serveraddr) Stream defaultProtocol
  listenSock <- socket (addrFamily serveraddr) Stream (addrProtocol serveraddr)
  bind listenSock $ addrAddress serveraddr
  setSocketOption listenSock ReuseAddr 1
  listen listenSock listenQueueLength
  return listenSock
  where
    listenQueueLength :: Int
    listenQueueLength = 8192 

worker :: Socket -> IO ()
worker sock = do
  -- putStrLn "starting..."
  recvRequest ""
  -- putStrLn "ttt"
  SB.sendAll sock reply
  -- putStrLn "kk"
  -- worker sock
  close sock
  performGC
  -- putStrLn "aaaa"
  where
    recvRequest bs = do
      s <- SB.recv sock 8192
      let t = B.append bs s
      if B.null s || "\r\n\r\n" `B.isInfixOf` t
        then return ()
        else recvRequest t
      

reply :: ByteString
reply = B.append fauxHeader fauxIndex
 
replyLen :: Int
replyLen = B.length reply 

fauxHeader :: ByteString
fauxHeader = pack s
  where
    s = "HTTP/1.1 200 OK\r\nDate: Tue, 09 Oct 2012 16:36:18 GMT\r\nContent-Length: 151\r\nServer: Mighttpd/2.8.1\r\nLast-Modified: Mon, 09 Jul 2012 03:42:33 GMT\r\nContent-Type: text/html\r\n\r\n"
 
fauxIndex :: ByteString
fauxIndex = pack s
  where
    s = "<html>\n<head>\n<title>Welcome to nginx!</title>\n</head>\n<body bgcolor=\"white\" text=\"black\">\n<center><h1>Welcome to nginx!</h1></center>\n</body>\n</html>\n"