{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Web.Spock.Worker.Internal.QueueTests
    (  htf_thisModulesTests )
where

import Web.Spock.Worker.Internal.Queue

import Test.Framework
import qualified Data.Map.Strict as M

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
