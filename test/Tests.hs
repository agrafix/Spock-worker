{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework
import {-@ HTF_TESTS @-} Web.Spock.Worker.Internal.QueueTests

main = htfMain htf_importedTests
