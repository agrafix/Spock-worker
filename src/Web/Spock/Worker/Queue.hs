module Web.Spock.Worker.Queue
    ( WorkerQueue, newQueue, size, enqueue, dequeue, isFull )
where

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
