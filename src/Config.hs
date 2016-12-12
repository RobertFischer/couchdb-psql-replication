{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}
{-# LANGUAGE DeriveGeneric #-}

module Config (module Config) where

import Control.Concurrent ( forkIO )
import Data.Default
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import System.IO
import GHC.Word

-- |Represents a location of a server on the Internet
data ServerAddress = ServerAddress
  {
    serverPort :: Word16,
    serverName :: String
  } deriving (Show, Read, Eq, Ord)

-- |Represents the overall configuration for the replication
data ReplConfig = ReplConfig
  {
    couchServer :: ServerAddress,
    psqlServer :: ServerAddress,
    startingSequence :: String,
    couchConcurrency :: Int,
    psqlConcurrency :: Int,
    pollLength :: Int,
    psqlSchemaName :: String,
    psqlUser :: String,
    psqlPass :: String,
    psqlDb :: String
  } deriving (Show, Read, Eq, Ord)

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
couchPort = fromIntegral . serverPort . couchServer

psqlHost :: ReplConfig -> String
psqlHost = serverName . psqlServer

psqlPort :: ReplConfig -> Word16
psqlPort = serverPort . psqlServer

doLog :: String -> String -> IO ()
doLog lvl str = do
    _ <- forkIO $ do
      hSetBuffering h LineBuffering
      hPutStr h $ "[" ++ lvl ++ "]\t" ++ str ++ "\n"
    return ()
  where
    h = stderr

logWarn :: String -> IO ()
logWarn = doLog "WARN"

logError :: String -> IO ()
logError = doLog "ERROR"

logInfo :: String -> IO ()
logInfo = doLog "INFO"
