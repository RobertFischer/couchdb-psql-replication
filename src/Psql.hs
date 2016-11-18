{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Psql (module Psql) where

import Data.ByteString.Lazy (ByteString)
import Codec.Compression.GZip
import qualified Database.PostgreSQL.Simple as Db
import Config (ReplConfig)
import Couch
(
  DbName(..),
  DocRev(..)
)

-- |Represents a client to the PSQL synchronization
type Client = Client
  {
    clientConn :: IO Db.Connection
  }

client :: ReplConfig -> IO Client
-- ^Generate a client based on the replication configuration.
client = do
  undefined

checkRevisionExists :: Client -> DbName -> DocRev -> IO Bool
-- ^Determine if a given revision exists within the given database.
checkRevisionExists = do
  undefined

withTransaction :: Client -> (Client -> IO a) -> IO a
-- ^Wraps 'Db.withTransaction' to use a connection from the given 'Client'. The
-- client passed into the second argument will always execute within the transaction.
withTransaction = do
  undefined

-- TODO Make this sensitive to the reported size of the attachment
-- buffer size = min of 1 MB and (attachment size + 1 byte)
-- compression level = 1 <= ( log base 2 of attachment size in MB ) <= 9
compress :: ByteString -> ByteString
compress = compressWith params
  where
    params = defaultCompressParams
      {
        compressLevel = bestCompression
        compressWindowBits = windowBits 15 -- Maximum window bits
        compressMemoryLevel = maxMemoryLevel
        compressBufferSize = 1024 * 1024
      }
