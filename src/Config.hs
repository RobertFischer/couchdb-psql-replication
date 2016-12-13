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
      couchConcurrency = 1,
      psqlConcurrency = 5,
      pollLength = 60 * 60,
      psqlSchemaName = "couch",
      psqlUser = connectUser defaultConnectInfo,
      psqlPass = connectPassword defaultConnectInfo,
      psqlDb = connectDatabase defaultConnectInfo
    }

class Parseable a where
  parseStr :: String -> a

instance Parseable Int where
  parseStr = read

instance Parseable String where
  parseStr = id

instance Parseable Word16 where
  parseStr = read

readConfig :: IO ReplConfig
-- ^Reads the configuration from the environment, with default values being
-- used if they are not specified.
readConfig = do
  logInfo "Loading configuration"
  ReplConfig <$>
    readCouchServerConfig <*>
    readPsqlServerConfig <*>
    startSeq <*>
    couchConc <*>
    psqlConc <*>
    pollLen <*>
    psqlSch <*>
    psqlUsr <*>
    psqlPss <*>
    psqlD
  where
    fromDefZ :: String -> (ReplConfig -> Int) -> IO Int
    fromDefZ = readWithDef
    fromDefS :: String -> (ReplConfig -> String) -> IO String
    fromDefS = readWithDef
    startSeq = fromDefS "COUCH_STARTING_SEQUENCE" startingSequence
    couchConc = fromDefZ "COUCH_CONCURRENCY" couchConcurrency
    psqlConc = fromDefZ "PSQL_CONCURRENCY" psqlConcurrency
    pollLen = fromDefZ "COUCH_POLL_SECONDS" pollLength
    psqlSch = fromDefS "PSQL_SCHEMA" psqlSchemaName
    psqlUsr = fromDefS "PSQL_USER" psqlUser
    psqlPss = fromDefS "PSQL_PASS" psqlPass
    psqlD = fromDefS "PSQL_DB" psqlDb

readWithDef :: (Parseable b, Show b) => String -> (ReplConfig -> b) -> IO b
readWithDef envVarName accessor = do
    logInfo $ "Loading configuration from " ++ envVarName ++ " -- default is " ++ (show defaultVal)
    maybeFoundStr <- (lookupEnv envVarName)
    let val = maybe defaultVal parseStr maybeFoundStr
    logInfo $ "Using value " ++ (show val) ++ " for " ++ envVarName ++ " configuration"
    return val
  where
    defaultVal = accessor def

readCouchServerConfig :: IO CouchServerAddress
readCouchServerConfig = do
  CouchServerAddress <$>
    (
      ServerAddress <$>
        ( readWithDef "COUCH_SERVER_PORT" (fromIntegral . couchPort) ) <*>
        ( readWithDef "COUCH_SERVER_HOST" couchHost )
    )

readPsqlServerConfig :: IO PsqlServerAddress
readPsqlServerConfig = do
  PsqlServerAddress <$>
    (
      ServerAddress <$>
        ( readWithDef "PSQL_SERVER_PORT" psqlPort ) <*>
        ( readWithDef "PSQL_SERVER_HOST" psqlHost )
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

doLog :: Handle -> String -> String -> IO ()
doLog h lvl str = do
    _ <- forkIO $ do
      hSetBuffering h LineBuffering
      hPutStr h $ "[" ++ lvl ++ "]\t" ++ str ++ "\n"
    return ()

doLogStderr :: String -> String -> IO ()
doLogStderr = doLog stderr

doLogStdout :: String -> String -> IO ()
doLogStdout = doLog stdout

logWarn :: String -> IO ()
logWarn = doLogStderr "WARN"

logError :: String -> IO ()
logError = doLogStderr "ERROR"

logInfo :: String -> IO ()
logInfo = doLogStdout "INFO"
