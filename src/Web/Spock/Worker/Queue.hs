{-# OPTIONS_GHC -F -pgmF htfpp #-}
module Web.Spock.Worker.Queue
    ( WorkerQueue, newQueue, size, enqueue, dequeue, isFull
    , htf_thisModulesTests
    )
where

import Test.Framework

import Control.Concurrent
import Control.Concurrent.STM
import Control.Applicative
import qualified Data.Map.Strict as M
import qualified Data.Vector as V

data WorkerQueue p v
   = WorkerQueue
   { wq_container :: TVar (M.Map p (V.Vector v))
   , wq_maxSize :: Int
   }

newQueue :: Int -> IO (WorkerQueue p v)
newQueue limit =
    (flip WorkerQueue) limit <$> newTVarIO M.empty

size :: WorkerQueue p v -> STM Int
size (WorkerQueue q _) =
    M.size <$> readTVar q

isFull :: WorkerQueue p v -> STM Bool
isFull wq@(WorkerQueue _ sizeLimit) =
    do currSize <- size wq
       return (currSize >= sizeLimit)

enqueue :: Ord p => p -> v -> WorkerQueue p v -> STM ()
enqueue priority value wq@(WorkerQueue q _) =
    do full <- isFull wq
       if full
       then retry
       else modifyTVar' q (M.insertWith (V.++) priority (V.singleton value))

dequeue :: Ord p => p -> WorkerQueue p v -> STM v
dequeue minP (WorkerQueue q _) =
    do m <- readTVar q
       if M.null m
       then retry
       else runDequeue m
    where
      runDequeue m =
          do let (minPrio, vals) = M.findMin m
             if minPrio <= minP
             then case V.toList vals of
                    [workEl] ->
                        do writeTVar q (M.delete minPrio m)
                           return workEl
                    (workEl:xs) ->
                        do writeTVar q (M.adjust (const (V.fromList xs)) minPrio m)
                           return workEl
                    [] ->
                        error "Library-Error: This should never happen."
             else retry

-- -------------
-- TESTS
-- -------------

test_enqueueDequeue =
    do q <- newQueue 10
       atomically $ enqueue (1 :: Int) True q
       val <- atomically $ dequeue 1 q
       assertEqual True val
       atomically $ enqueue (10 :: Int) False q
       forkIO $ atomically $ enqueue 4 True q
       val2 <- atomically $ dequeue 5 q
       assertEqual True val2

test_isFull =
    do q <- newQueue 2
       atomically $ enqueue (1 :: Int) True q
       fullA <- atomically $ isFull q
       atomically $ enqueue (2 :: Int) False q
       fullB <- atomically $ isFull q
       assertEqual False fullA
       assertEqual True fullB
