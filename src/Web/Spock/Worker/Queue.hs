{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Web.Spock.Worker.Queue
    ( WorkerQueue, newQueue, size, enqueue, dequeue, isFull
    , toListPQ, fromListPQ
    , htf_thisModulesTests
    )
where

import Test.Framework

import Control.Monad
import Control.Concurrent.STM
import Control.Applicative
import Data.Maybe

import qualified Data.Map.Strict as M
import qualified Data.Vector as V

data PureQueue p v
   = PureQueue
   { pq_container :: !(M.Map p (V.Vector v))
   , pq_maxSize :: !Int
   } deriving (Show, Eq)

emptyPQ :: Int -> PureQueue p v
emptyPQ maxQueueSize =
    PureQueue M.empty maxQueueSize

sizePQ :: PureQueue p v -> Int
sizePQ (PureQueue m _) =
    M.size m

isFullPQ :: PureQueue p v -> Bool
isFullPQ pq =
    sizePQ pq >= (pq_maxSize pq)

toListPQ :: Ord p => PureQueue p v -> [(p, [v])]
toListPQ (PureQueue m _) =
    map (\(k, v) -> (k, V.toList v)) (M.toList m)

fromListPQ :: Ord p => Int -> [(p, [v])] -> Maybe (PureQueue p v)
fromListPQ limit kv
    | length kv > limit = Nothing
    | otherwise =
        Just $
        foldl (\(PureQueue content _) (k, v) ->
                   (PureQueue (M.insert k (V.fromList v) content) limit)
              ) (emptyPQ limit) kv

maxPrioPQ :: Ord p => PureQueue p v -> p
maxPrioPQ (PureQueue m _) =
    fst (M.findMax m)

enqueuePQ :: Ord p => p -> v -> PureQueue p v -> (Bool, PureQueue p v)
enqueuePQ prio value pq@(PureQueue m _) =
    if isFullPQ pq
    then ( False, pq )
    else ( True
         , pq
           { pq_container =
                 M.insertWith (V.++) prio (V.singleton value) m
           }
         )

dequeuePQ :: Ord p => p -> PureQueue p v -> (Maybe v, PureQueue p v)
dequeuePQ bound pq =
    removePrio $ dequeuePQ' bound pq
    where
      removePrio (Nothing, q) = (Nothing, q)
      removePrio (Just (_, v), q) = (Just v, q)

dequeuePQ' :: Ord p => p -> PureQueue p v -> (Maybe (p, v), PureQueue p v)
dequeuePQ' prioBound pq@(PureQueue m _)
    | M.null m = (Nothing, pq)
    | minK > prioBound = (Nothing, pq)
    | otherwise =
        case V.toList vec of
          [workEl] ->
              (Just (minK, workEl), updatePQ (M.delete minK))
          (workEl:xs) ->
              (Just (minK, workEl), updatePQ (M.adjust (const (V.fromList xs)) minK))
          [] ->
              error "Library-Error: This should never happen."
    where
      (minK, vec) =
          M.findMin m
      updatePQ fun =
          pq { pq_container = fun (pq_container pq) }

newtype WorkerQueue p v =
    WorkerQueue (TVar (PureQueue p v))

newQueue :: Int -> IO (WorkerQueue p v)
newQueue limit =
    WorkerQueue <$> newTVarIO (emptyPQ limit)

size :: WorkerQueue p v -> STM Int
size (WorkerQueue qVar) =
    readTVar qVar >>= (return . sizePQ)

isFull :: WorkerQueue p v -> STM Bool
isFull (WorkerQueue qVar) =
    readTVar qVar >>= (return . isFullPQ)

enqueue :: Ord p => p -> v -> WorkerQueue p v -> STM ()
enqueue prio value (WorkerQueue qVar) =
    do q <- readTVar qVar
       let (ok, newQ) = enqueuePQ prio value q
       if ok
       then writeTVar qVar newQ
       else retry

dequeue :: Ord p => p -> WorkerQueue p v -> STM (Maybe v)
dequeue minP (WorkerQueue qVar) =
    do q <- readTVar qVar
       let (mVal, newQ) = dequeuePQ minP q
       when (isJust mVal) $ writeTVar qVar newQ
       return mVal

-- -------------
-- TESTS
-- -------------

tAddToMap :: Ord k => k -> a -> M.Map k [a] -> M.Map k [a]
tAddToMap k val m =
    M.insertWith (++) k [val] m

tDeq :: Ord k => k -> PureQueue k a -> M.Map k [a]
tDeq maxP q
    | sizePQ q == 0 =
        M.empty
    | otherwise =
        let (mVal, newQ) = dequeuePQ' maxP q
        in case mVal of
             Nothing ->
                 M.empty
             Just (k, val) ->
                 tAddToMap k val (tDeq maxP newQ)

tMappifyInput :: Ord k => [(k, a)] -> M.Map k [a]
tMappifyInput xs =
    foldl (\m (k, v) ->
               tAddToMap k v m
          ) M.empty xs

prop_enqueueDequeuePQ :: [(Int, Int)] -> Bool
prop_enqueueDequeuePQ xs =
    let pq = foldl (\q (prio :: Int, el :: Int) ->
                        let (ok, newPQ) = enqueuePQ prio el q
                        in if ok then newPQ else (error "Failed to enqueue!")
                   ) (emptyPQ (length xs)) xs
        maxP = maxPrioPQ pq
    in (tMappifyInput xs == tDeq maxP pq)

prop_onlyDequeueBelowPrio :: Int -> [(Int, Int)] -> Bool
prop_onlyDequeueBelowPrio prio xs =
    let xs' = M.toList $ tMappifyInput xs
        Just pq = fromListPQ (length xs) (xs' :: [(Int, [Int])])
        filtered = filter (\(p, _) -> p <= prio) xs
    in (tMappifyInput filtered == tDeq prio pq)

prop_isFull :: Int -> [(Int, Int)] -> Property
prop_isFull limit xs =
    limit > 0 ==>
    let xs' = M.toList $ tMappifyInput xs
        mPq = fromListPQ limit (xs' :: [(Int, [Int])])
    in case mPq of
         Just pq ->
             if limit == (length xs')
             then isFullPQ pq
             else limit > (length xs')
         Nothing ->
             limit < (length xs')

test_dontEnqueueIfFull :: IO ()
test_dontEnqueueIfFull =
    let pq = emptyPQ 0
        (ok, newPQ) = enqueuePQ (0 :: Int) False pq
    in do assertBool (not ok)
          assertEqual pq newPQ
