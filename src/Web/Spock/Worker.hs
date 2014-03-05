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

import Control.Monad (forever)
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
       _ <- liftIO $ forkIO (runSpockIO heart $ core q)
       return (WorkQueue q)
    where
      core q =
          do now <- liftIO $ getCurrentTime
             work <- liftIO $ atomically $ Q.dequeue now q
             res <-
                 do workRes <- EX.catch (runErrorT $ workHandler work)
                               (\(e::SomeException) -> return $ Left (show e))
                    case workRes of
                      Left err -> liftIO (errorHandler err work)
                      Right r -> return r
             case res of
               WorkRepeatIn secs ->
                   addWork (WorkIn secs) work (WorkQueue q)
               WorkRepeatAt time ->
                   addWork (WorkAt time) work (WorkQueue q)
               _ ->
                   return ()
             core q

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

-- -------------
-- TESTS
-- -------------

test_worker =
    initSpock $ \workVar ->
    do let checkQueue = checkQueueImpl workVar
       worker <- newWorker 15 (workHandler workVar) errorHandler
       addWork WorkNow (1 :: Int) worker
       sleepMS 1000
       checkQueue [1]
       liftIO $
              do me <- myThreadId
                 killThread me
    where
      sleepMS t =
          liftIO $ threadDelay (t * 1000)
      checkQueueImpl var exp =
          liftIO $
          do v <- atomically $! readTVar var
             assertEqual exp v
      initSpock a =
          do workResults <- newTVarIO []
             spock 15150 (SessionCfg "test" 123 42) conn () (a workResults)
      conn =
          PCConn $ ConnBuilder (return ()) (const (return ())) pcfg
      pcfg =
          PoolCfg 1 1 5
      errorHandler errMsg _ =
          error errMsg
      workHandler workVar job =
          do liftIO $ atomically $ modifyTVar' workVar (\c -> c ++ [job])
             return WorkComplete
