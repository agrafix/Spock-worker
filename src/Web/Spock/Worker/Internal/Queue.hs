module Web.Spock.Worker.Internal.Queue where

import           Control.Arrow          (second)
import           Control.Concurrent.STM
import           Control.Monad
import qualified Data.Map.Strict        as M
import           Data.Maybe
import qualified Data.Vector            as V

data PureQueue p v
   = PureQueue
   { pq_container :: !(M.Map p (V.Vector v))
   , pq_maxSize   :: !Int
   } deriving (Show, Eq)

emptyPQ :: Int -> PureQueue p v
emptyPQ = PureQueue M.empty

sizePQ :: PureQueue p v -> Int
sizePQ (PureQueue m _) =
    M.size m

isFullPQ :: PureQueue p v -> Bool
isFullPQ pq =
    sizePQ pq >= pq_maxSize pq

toListPQ :: PureQueue p v -> [(p, [v])]
toListPQ (PureQueue m _) =
    map (second V.toList) (M.toList m)

fromListPQ :: Ord p => Int -> [(p, [v])] -> Maybe (PureQueue p v)
fromListPQ limit kv
    | length kv > limit = Nothing
    | otherwise =
        Just $
        foldl (\(PureQueue content _) (k, v) ->
                   (PureQueue (M.insert k (V.fromList v) content) limit)
              ) (emptyPQ limit) kv

maxPrioPQ :: PureQueue p v -> p
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
size (WorkerQueue qVar) = liftM sizePQ (readTVar qVar)

isFull :: WorkerQueue p v -> STM Bool
isFull (WorkerQueue qVar) = liftM isFullPQ (readTVar qVar)

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
