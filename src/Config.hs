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
import Database.PostgreSQL.Simple ( ConnectInfo(..), defaultConnectInfo )
import System.Environment ( lookupEnv )
import Data.Maybe ( maybe )

-- |Represents a location of a server on the Internet
data ServerAddress = ServerAddress
  {
    serverPort :: Word16,
    serverName :: String
  } deriving (Show, Read, Eq, Ord)

newtype CouchServerAddress = CouchServerAddress ServerAddress deriving (Show, Read, Eq, Ord)
unwrapCouchServer :: CouchServerAddress -> ServerAddress
unwrapCouchServer (CouchServerAddress addr) = addr

newtype PsqlServerAddress = PsqlServerAddress ServerAddress deriving (Show, Read, Eq, Ord)
unwrapPsqlServer :: PsqlServerAddress -> ServerAddress
unwrapPsqlServer (PsqlServerAddress addr) = addr

-- |Represents the overall configuration for the replication
data ReplConfig = ReplConfig
  {
    couchServer :: CouchServerAddress,
    psqlServer :: PsqlServerAddress,
    startingSequence :: String,
    couchConcurrency :: Int,
    psqlConcurrency :: Int,
    pollLength :: Int,
    psqlSchemaName :: String,
    psqlUser :: String,
    psqlPass :: String,
    psqlDb :: String
  } deriving (Show, Read, Eq, Ord)

instance Default CouchServerAddress where
  def = CouchServerAddress $ ServerAddress
    { -- TODO Derive these from some library
      serverPort = 5984,
      serverName = "localhost"
    }

instance Default PsqlServerAddress where
  def = PsqlServerAddress $ ServerAddress
    {
      serverPort = connectPort defaultConnectInfo,
      serverName = connectHost defaultConnectInfo
    }

instance Default ReplConfig where
  def = ReplConfig
    {
      couchServer = def,
      psqlServer = def,
      startingSequence = "now",
      couchConcurrency = 10,
      psqlConcurrency = 100,
      pollLength = 60 * 60,
      psqlSchemaName = "couch",
      psqlUser = connectUser defaultConnectInfo,
      psqlPass = connectPassword defaultConnectInfo,
      psqlDb = connectDatabase defaultConnectInfo
    }

readConfig :: IO ReplConfig
-- ^Reads the configuration from the environment, with default values being
-- used if they are not specified.
readConfig =
  ReplConfig <$>
    readCouchServerConfig <*>
    readPsqlServerConfig <*>
    fromDef "COUCH_STARTING_SEQUENCE" id startingSequence <*>
    fromDef "COUCH_CONCURRENCY" read couchConcurrency <*>
    fromDef "PSQL_CONCURRENCY" read psqlConcurrency <*>
    fromDef "COUCH_POLL_SECONDS" read pollLength <*>
    fromDef "PSQL_SCHEMA" id psqlSchemaName <*>
    fromDef "PSQL_USER" id psqlUser <*>
    fromDef "PSQ_PASS" id psqlPass <*>
    fromDef "PSQL_DB" id psqlDb
  where
    fromDef = readWithDef

readWithDef :: String -> (String -> b) -> (ReplConfig -> b) -> IO b
readWithDef envVarName valConverter accessor = do
  let defaultVal = accessor def
  maybe defaultVal valConverter <$> lookupEnv envVarName

readCouchServerConfig :: IO CouchServerAddress
readCouchServerConfig = do
  CouchServerAddress <$>
    (
      ServerAddress <$>
        ( readWithDef "COUCH_SERVER_PORT" read (fromIntegral . couchPort) ) <*>
        ( readWithDef "COUCH_SERVER_HOST" id   couchHost )
    )

readPsqlServerConfig :: IO PsqlServerAddress
readPsqlServerConfig = do
  PsqlServerAddress <$>
    (
      ServerAddress <$>
        ( readWithDef "PSQL_SERVER_PORT" read psqlPort ) <*>
        ( readWithDef "PSQL_SERVER_HOST" id   psqlHost )
    )

couchHostBS :: ReplConfig -> B.ByteString
couchHostBS = B8.pack . couchHost

couchHost :: ReplConfig -> String
couchHost = serverName . unwrapCouchServer . couchServer

couchPort :: ReplConfig -> Int
couchPort = fromIntegral . serverPort . unwrapCouchServer . couchServer

psqlHost :: ReplConfig -> String
psqlHost = serverName . unwrapPsqlServer . psqlServer

psqlPort :: ReplConfig -> Word16
psqlPort = serverPort . unwrapPsqlServer . psqlServer

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
