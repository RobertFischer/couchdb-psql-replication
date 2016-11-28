{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Psql (module Psql) where

import Codec.Compression.GZip
import Config ( ReplConfig )
import Couch ( DbName(..), DocRev(..), DocId(..), RevId(..), DocDetails, AttachmentStub )
import Data.ByteString.Lazy
import qualified Database.PostgreSQL.Simple as Db

-- |Represents a client to the PSQL synchronization
data Client = Client
  {
    getConn :: IO Db.Connection
  }

-- |Represents a record of a revision (without any content)
data RevRecord = RevRecord
  {
    revRecordDb :: DbName,
    docRev :: DocRev
  }

revRecordDoc :: RevRecord -> DocId
revRecordDoc = Couch.docId . docRev

revRecordRev :: RevRecord -> RevId
revRecordRev = Couch.docRevId . docRev

makeClient :: ReplConfig -> IO Client
-- ^Generate a client based on the replication configuration.
makeClient = do
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
        compressLevel = bestCompression,
        compressWindowBits = windowBits 15, -- Maximum window bits
        compressMemoryLevel = maxMemoryLevel,
        compressBufferSize = 1024 * 1024 -- Used only for the initial buffer; always 16k after that
      }

ensureDatabase :: Client -> DbName -> IO ()
ensureDatabase = do
  undefined

ensureDocument :: Client -> DbName -> DocId -> IO ()
ensureDocument = do
  undefined

ensureRevision :: Client -> DbName -> DocRev -> IO ()
ensureRevision = do
  undefined

ensureRevisionContent :: Client -> DbName -> DocRev -> DocDetails -> IO ()
ensureRevisionContent = do
  undefined

noteDeletedRevision :: Client -> DbName -> DocRev -> IO ()
noteDeletedRevision = do
  undefined

noteDeletedDocument :: Client -> DbName -> DocId -> IO ()
noteDeletedDocument = do
  undefined

ensureAttachment :: Client -> DbName -> DocId -> AttachmentStub -> IO ()
ensureAttachment = do
  undefined

ensureAttachmentContent :: Client -> DbName -> DocId -> AttachmentStub -> ByteString -> IO ()
ensureAttachmentContent = do
  undefined

fetchLiveRevisions :: Client -> IO [RevRecord]
fetchLiveRevisions = do
  undefined

