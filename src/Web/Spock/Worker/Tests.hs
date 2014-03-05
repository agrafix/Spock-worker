{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework
import {-@ HTF_TESTS @-} Web.Spock.Worker.Queue
import {-@ HTF_TESTS @-} Web.Spock.Worker

main = htfMain htf_importedTests
