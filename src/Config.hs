{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}
{-# LANGUAGE DeriveGeneric #-}

module Config where

import Data.Default
import GHC.Generics
import Data.ByteString as B
import Data.ByteString.Char8 as B8

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
    startingSequence :: String,
    couchConcurrency :: Int,
    psqlConcurrency :: Int,
    pollLength :: Int
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

couchHostBS :: ReplConfig -> B.ByteString
couchHostBS = B8.pack . couchHost

couchHost :: ReplConfig -> String
couchHost = serverName . couchServer

couchPort :: ReplConfig -> Int
couchPort = serverPort . couchServer
