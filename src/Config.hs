{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}
{-# LANGUAGE DeriveGeneric #-}

module Config where

import Util
import Data.Default
import GHC.Generics

-- |Represents a location of a server on the Internet
data ServerAddress = ServerAddress
  {
    serverPort :: Int,
    serverName :: String
  } deriving (Generic, Show, Read, Eq, Ord)

-- |Represents the overall configuration for the replication
data ReplConfig = ReplConfig
  {
    couchServer :: ServerAddress,
    psqlServer :: ServerAddress,
    startingSequence :: String -- default: "now"
  } deriving (Generic, Show, Read, Eq, Ord)

defaultConfig :: ReplConfig
-- ^The default configuration for replication
defaultConfig = undefined

instance Default ReplConfig where
  def = defaultConfig

readConfig :: IO ReplConfig
-- ^Reads the configuration from the environment, with default values being
-- used if they are not specified.
readConfig = do
  undefined

