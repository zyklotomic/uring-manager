module Control.Concurrent.URingManager where

import qualified System.Linux.IO.URing as URing
import Data.Unique
-- because we can't import GHC.Event.IntTable
import qualified Data.IntMap.Strict as IntMap
import Data.Int
import Control.Concurrent (myThreadId, getNumCapabilities, rtsSupportsBoundThreads
                          , threadCapability)
import Control.Concurrent.MVar
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef)   
import qualified System.Linux.IO.URing as URing 

import System.Linux.IO.URing.Sqe (SqeBuilder(..), UserData, SqeIndex)
import System.Linux.IO.URing.Cqe (cqeRes, cqeUserData)
import System.Linux.IO.URing.Ring (getSqe, sqePtr, pushSqe)

import System.IO.Unsafe (unsafePerformIO)
import GHC.Conc.Sync (ThreadId, labelThread, forkOn, yield)
import GHC.IOArray
import Control.Monad (forM_, void)
import Control.Exception (catch)
import Data.ByteString as BS

type Completion = Int32 -> IO ()

data PendingReq = PendingReq !SqeIndex !Completion !BS.ByteString

data URingManager = URingManager
    { uring :: !URing.URing
    , requests :: !(IORef (IntMap.IntMap PendingReq))
    , queueDepth :: !Int
    , tableLock :: !(MVar ())
    }
 
supportsIOURing :: Bool
supportsIOURing = unsafePerformIO checkIOURing
{-# NOINLINE supportsIOURing #-}

checkIOURing :: IO Bool
checkIOURing
  | not rtsSupportsBoundThreads = return False
  | otherwise = do
    catch (startURingManagerThreads >> return True) uhOh
    -- TODO: Cleanup
  where
    uhOh :: IOError -> IO Bool
    uhOh _ioe = return False

  --     | UnsupportedOperation <- ioe_type ioe = return False
  --     | otherwise = do
  --       puts $ "Unexpected error while checking io-uring support: " ++ show ioe
  --       return False

initDepth :: Int
initDepth = 8192

-- Required to make changes to any manager
uringManagerLock :: MVar ()
uringManagerLock = unsafePerformIO $ do
    putStrLn "Initializing uringManagerLock..."
    newMVar ()
{-# NOINLINE uringManagerLock #-}

uringManager :: IORef (IOArray Int (Maybe (ThreadId, URingManager)))
uringManager = unsafePerformIO $ do
    putStrLn "Initializing uringManager..."
    numCaps <- getNumCapabilities
    uringManagerArray <- newIOArray (0, numCaps - 1) Nothing
    um <- newIORef uringManagerArray
    -- TODO: SharedCAF
    return um
{-# NOINLINE uringManager #-}

-- TODO: Error checking
getSystemURingManager_ :: IO URingManager
getSystemURingManager_ = do
    t <- myThreadId 
    uringManagerArray <- readIORef uringManager
    (cap, _) <- threadCapability t
    Just mgr <- fmap snd `fmap` readIOArray uringManagerArray cap
    return mgr

withURingManager :: (URingManager -> IO ()) -> IO ()
withURingManager cb = do
    um <- getSystemURingManager_
    cb um

newURingManager :: IO URingManager
newURingManager = do
    ring <- URing.newURing initDepth
    reqs <- newIORef IntMap.empty
    lock <- newMVar ()
    let mgr = URingManager { uring = ring
                           , requests = reqs
                           , queueDepth = initDepth
                           , tableLock = lock
                           }
    return mgr

startURingManagerThreads :: IO ()
startURingManagerThreads
    | not rtsSupportsBoundThreads = error "Not threaded RTS!"
    | otherwise = withMVar uringManagerLock $ \_ -> do
        uringManagerArray <- readIORef uringManager
        let (_, high) = boundsIOArray uringManagerArray 
        forM_ [0..high] $ \i -> do
            um <- newURingManager 
            !tid <- forkOn i $ startURingManager um
            writeIOArray uringManagerArray i (Just (tid,um)) 

startURingManager :: URingManager -> IO ()
startURingManager mgr = do
    tid <- myThreadId
    labelThread tid "uring completion thread"
    putStrLn $ "URingManager initialized on thread " ++ show tid
    go
  where
    go = do
        -- putStrLn $ "(" ++ show tid ++ "): one iteration " ++ show u ++ " " ++ show sz
        -- putStrLn $ "iter " ++ show u
        -- if u `mod` 100000 == 0
        --     then withMVar (tableLock mgr) $ \_ -> do
        --         putStrLn $ "--- PRINTING ENTRIES --- " ++  show u
        --         im <- readIORef (requests mgr)
        --         forM_ im $ \(PendingReq _ _ bs) -> print bs
        --         putStrLn "------------------------"
        --         putStrLn $ "count: " ++  show sz
        --     else pure ()

        maybeCqe <- URing.popCq (uring mgr)
        case maybeCqe of
            Nothing -> yield
            Just cqe -> do
                -- tid <- myThreadId
                -- u <- hashUnique <$> newUnique
                -- sz <- IntMap.size <$> readIORef (requests mgr) 
                let reqId :: Int
                    reqId = fromIntegral $ cqeUserData cqe
                mb_req <- deleteRequest reqId
                case mb_req of
                    Nothing -> error $ "No request for reqId " ++ show reqId
                    Just (PendingReq sqe_idx compl _) -> do
                        URing.freeSqe (uring mgr) sqe_idx
                        -- tryPutMVar (freeSqSlot mgr) () -- TODO: Slow?
                        compl (cqeRes cqe)
        go

    deleteRequest :: Int -> IO (Maybe PendingReq)
    deleteRequest reqId = do
        -- putStrLn "Trying to take mvar del"
        _ <- takeMVar (tableLock mgr)
        -- putStrLn "taken del"
        im <- readIORef (requests mgr)
        let res = IntMap.lookup reqId im
        writeIORef (requests mgr) $ IntMap.delete reqId im
        putMVar (tableLock mgr) ()
        -- putStrLn "returned del"
        return res

submitWithCompletion :: BS.ByteString -> (UserData -> SqeBuilder ()) -> Completion -> IO ()
submitWithCompletion bs mkSqe compl = do
    -- mvar <- newEmptyMVar
    -- let compl = putMVar mvar
    withURingManager $ \mgr -> do
        u <- hashUnique <$> newUnique
        -- putStrLn $ "unique: " ++ show u
        sqeIdx <- waitUntilSqeFree mgr
        insertReq mgr u (PendingReq sqeIdx compl bs)
        let sqe = mkSqe (fromIntegral u)
        _r <- pokeSqe sqe (sqePtr (uring mgr) sqeIdx)
        pushRes <- pushSqe (uring mgr) sqeIdx
        if pushRes
            then void $ URing.submit (uring mgr) 1 Nothing
            -- https://gitlab.haskell.org/ghc/ghc/-/merge_requests/8073#note_515067
            else error "submitAndWait: this shouldn't happen"  
    where
        dup_uniq_err = error "repeated IO request unique"

        waitUntilSqeFree :: URingManager -> IO SqeIndex
        waitUntilSqeFree mgr = do
            maybeSqeIdx <- getSqe (uring mgr)
            case maybeSqeIdx of
                Nothing -> yield >> waitUntilSqeFree mgr
                Just idx -> return idx

        -- TODO: Need lock here
        insertReq :: URingManager -> Int -> PendingReq -> IO ()
        insertReq mgr k v = do
            -- putStrLn "trying to take mvar ins"
            _ <- takeMVar (tableLock mgr)
            -- putStrLn "taken insert"
            modifyIORef (requests mgr) $ \im ->
                IntMap.insertWith (dup_uniq_err) k v im
            putMVar (tableLock mgr) ()
            -- putStrLn "returend insert"

submitBlocking :: BS.ByteString -> (UserData -> SqeBuilder ()) -> IO Int32
submitBlocking bs mkSqe = do
    mvar <- newEmptyMVar
    let compl = putMVar mvar
    submitWithCompletion bs mkSqe compl
    takeMVar mvar
