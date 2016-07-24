{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Web.Spock.Worker
    ( -- * Define a Worker
      WorkHandler
    , WorkerConfig (..)
    , WorkerConcurrentStrategy (..)
    , WorkerDef (..)
    , newWorker
    , WorkResult (..)
      -- * Enqueue work
    , WorkQueue, WorkExecution (..), addWork
      -- * Error Handeling
    , ErrorHandler(..), InternalError (..)
    )
where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Error
import Control.Exception.Lifted as EX
import Control.Monad
import Control.Monad.Trans
import Data.Time
import Web.Spock
import qualified Web.Spock.Worker.Internal.Queue as Q

-- | An error from a worker
data InternalError a
   = InternalErrorMsg String
   | InternalError a

-- | Describe how you want to handle errors. Make sure you catch all exceptions
-- that can happen inside this handler, otherwise the worker will crash!
data ErrorHandler conn sess st err a
   = ErrorHandlerIO ((InternalError err) -> a -> IO WorkResult)
   | ErrorHandlerSpock ((InternalError err) -> a -> (WebStateM conn sess st) WorkResult)

-- | Describe how you want jobs in the queue to be performed
type WorkHandler conn sess st err a
   = a -> ExceptT (InternalError err) (WebStateM conn sess st) WorkResult

-- | The queue containing scheduled jobs
newtype WorkQueue a
   = WorkQueue { _unWorkQueue :: Q.WorkerQueue UTCTime a }

-- | Describes when a job should be executed
data WorkExecution
   = WorkNow
   | WorkIn NominalDiffTime
   | WorkAt UTCTime

-- | Describes the outcome of a job after completion. You can repeat jobs
data WorkResult
   = WorkComplete
   | WorkError
   | WorkRepeatIn NominalDiffTime
   | WorkRepeatAt UTCTime
   deriving (Show, Eq)

-- | Configure the concurrent behaviour of a worker. If you want tasks executed
-- concurrently, consider using 'WorkerConcurrentBounded'
data WorkerConcurrentStrategy
   = WorkerNoConcurrency
   | WorkerConcurrentBounded Int
   | WorkerConcurrentUnbounded

-- | Configure how the worker handles it's task and define the queue size
data WorkerConfig
   = WorkerConfig
   { wc_queueLimit :: Int
   , wc_concurrent :: WorkerConcurrentStrategy
   }

-- | Define a worker
data WorkerDef conn sess st err a
   = WorkerDef
   { wd_config       :: WorkerConfig
   , wd_handler      :: WorkHandler conn sess st err a
   , wd_errorHandler :: ErrorHandler conn sess st err a
   }

-- | Create a new background worker and limit the size of the job queue.
newWorker :: (MonadTrans t, Monad (t (WebStateM conn sess st)))
          => WorkerDef conn sess st err a
          -> t (WebStateM conn sess st) (WorkQueue a)
newWorker wdef =
    do let wc = wd_config wdef
           workHandler = wd_handler wdef
           errorHandler = wd_errorHandler wdef
       heart <- getSpockHeart
       q <- lift . liftIO $ Q.newQueue (wc_queueLimit wc)
       _ <- lift . liftIO $ forkIO (workProcessor q workHandler errorHandler heart (wc_concurrent wc))
       return (WorkQueue q)

workProcessor :: Q.WorkerQueue UTCTime a
              -> WorkHandler conn sess st err a
              -> ErrorHandler conn sess st err a
              -> WebState conn sess st
              -> WorkerConcurrentStrategy
              -> IO ()
workProcessor q workHandler errorHandler spockCore concurrentStrategy =
    do runningTasksVar <- newTVarIO 0
       loop runningTasksVar
    where
      runWork work =
          do workRes <-
                 EX.catch (runSpockIO spockCore $ runExceptT $ workHandler work)
                       (\(e::SomeException) -> return $ Left (InternalErrorMsg $ show e))
             case workRes of
               Left errMsg ->
                   case errorHandler of
                     ErrorHandlerIO h ->
                         h errMsg work
                     ErrorHandlerSpock h ->
                         runSpockIO spockCore $ h errMsg work
               Right r -> return r
      loop runningTasksV =
          do now <- getCurrentTime
             mWork <- atomically $ Q.dequeue now q
             case mWork of
               Nothing ->
                   do threadDelay (1000 * 1000) -- 1 sec
                      loop runningTasksV
               Just work ->
                   do case concurrentStrategy of
                        WorkerConcurrentBounded limit ->
                            do atomically $
                                 do runningTasks <- readTVar runningTasksV
                                    when (runningTasks >= limit) retry
                               _ <- forkIO $ launchWork runningTasksV work
                               return ()
                        WorkerNoConcurrency ->
                            launchWork runningTasksV work
                        WorkerConcurrentUnbounded ->
                            do _ <- forkIO $ launchWork runningTasksV work
                               return ()
                      loop runningTasksV

      launchWork runningTasksV work =
          do atomically $ modifyTVar runningTasksV (+ 1)
             res <- (runWork work `EX.finally` (atomically $ modifyTVar runningTasksV (\x -> x - 1)))
             case res of
               WorkRepeatIn secs ->
                   addWork (WorkIn secs) work (WorkQueue q)
               WorkRepeatAt time ->
                   addWork (WorkAt time) work (WorkQueue q)
               _ ->
                   return ()

-- | Add a new job to the background worker. If the queue is full this will block
addWork :: MonadIO m => WorkExecution -> a -> WorkQueue a -> m ()
addWork we work (WorkQueue q) =
    liftIO $
    do now <- getCurrentTime
       let execTime =
               case we of
                 WorkNow -> now
                 WorkIn later -> addUTCTime later now
                 WorkAt ts -> ts
       atomically $ Q.enqueue execTime work q
