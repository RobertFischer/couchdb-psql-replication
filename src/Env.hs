{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Env (module Env) where

import qualified Data.Char as Char
import System.Environment (lookupEnv)

readEnv :: Read a => String -> a -> IO a
-- ^Given a key and a default value, return either the value of the environment
-- variable with that key, or the default value if the environment variable is
-- not set, or is empty, or only contains whitespace.
readEnv key def =
    doRead `fmap` lookupEnv key
  where
    doRead Nothing       = def
    doRead Just(val)
      | isEmpty val      = def
      | isWhitespace val = def
      | otherwise        = read val
    isEmpty = null
    isWhitespace = all Char.isSpace
