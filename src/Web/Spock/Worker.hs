{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE RankNTypes, ScopedTypeVariables, OverloadedStrings #-}
module Web.Spock.Worker
    ( -- * Worker
      WorkQueue
    , WorkHandler
    , newWorker
    , addWork
    , WorkExecution (..)
    , WorkResult (..)
      -- * Error Handeling
    , ErrorHandler, InternalError
      -- * Tests
    , htf_thisModulesTests
    )
where

import Test.Framework

import Control.Concurrent
import Control.Concurrent.STM

import Control.Monad.Trans
import Control.Monad.Trans.Error
import Control.Exception.Lifted as EX
import Control.Exception

import Data.Time
import Web.Spock
import qualified Web.Spock.Worker.Queue as Q

type InternalError = String

-- | Describe how you want to handle errors. Make sure you catch all exceptions
-- that can happen inside this handler, otherwise the worker will crash!
type ErrorHandler a
   = InternalError -> a -> IO WorkResult

-- | Describe how you want jobs in the queue to be performed
type WorkHandler conn sess st a
   = a -> ErrorT InternalError (WebStateM conn sess st) WorkResult

-- | The queue containing scheduled jobs
newtype WorkQueue a
   = WorkQueue { unWorkQueue :: Q.WorkerQueue UTCTime a }

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

-- | Create a new background worker and limit the size of the job queue.
newWorker :: Int
          -> WorkHandler conn sess st a
          -> ErrorHandler a
          -> SpockM conn sess st (WorkQueue a)
newWorker maxSize workHandler errorHandler =
    do heart <- getSpockHeart
       q <- liftIO $ Q.newQueue maxSize
       _ <- liftIO $ forkIO (workProcessor q workHandler errorHandler heart)
       return (WorkQueue q)

workProcessor :: Q.WorkerQueue UTCTime a
              -> WorkHandler conn sess st a
              -> ErrorHandler a
              -> WebState conn sess st
              -> IO ()
workProcessor q workHandler errorHandler spockCore =
    loop
    where
      runWork work =
          do workRes <-
                 EX.catch (runSpockIO spockCore $ runErrorT $ workHandler work)
                       (\(e::SomeException) -> return $ Left (show e))
             case workRes of
               Left err -> errorHandler err work
               Right r -> return r
      loop =
          do now <- getCurrentTime
             mWork <- atomically $ Q.dequeue now q
             case mWork of
               Nothing ->
                   do threadDelay (1000 * 1000) -- 1 sec
                      loop
               Just work ->
                   do res <- runWork work
                      case res of
                        WorkRepeatIn secs ->
                            addWork (WorkIn secs) work (WorkQueue q)
                        WorkRepeatAt time ->
                            addWork (WorkAt time) work (WorkQueue q)
                        _ ->
                            return ()
                      loop

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
