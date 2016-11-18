{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Main where

import qualified Couch
import Couch (DocId, DbName, DbPage)
import qualified Psql
import qualified Config
import Util (backgroundTask)
import Codec.Compression.GZip

type CouchClient = Couch.Client
type PsqlClient = Psql.Client

-- TODO Attach EKG

main :: IO ()
main = do
  config <- Config.readConfig
  psql <- Psql.client config
  couch <- Couch.client config
  let upDoc = updateDocument couch psql
  dbs <- Couch.getAllDbs couch
  let bgPoll = Couch.pollCanges couch dbs upDoc
  withAwait bgPoll $ \poller -> do
    let upDb = processDatabase couch psql
    _ <- mapConcurrently upDb dbs
    link poller -- Do this last to avoid explosions until the end

processDocument :: CouchClient -> PsqlClient -> DocId -> IO ()
-- ^Responsible for synchronizing a specific document
processDocument = undefined

processDatabase :: CouchClient -> PsqlClient -> DbName -> IO ()
-- ^Responsible for synchronizing a specific revision
processDatabase couch psql db = do
    Psql.ensureDatabase psql db
    firstPage <- getFirstPage
    processDatabasePage couch psql db firstPage
    return ()
  where
    getFirstPage = Couch.getPage couch db None

processDatabasePage :: CouchClient -> PsqlClient -> DbName -> DbPage -> IO ()
-- ^Responsible for processing a given database page, including recursing into subsequent pages
processDatabasePage couch psql db page =
    (missingRevs, presentRevs) <- checkRevs page
    _ <- mapConcurrently processDoc $ map revDocId missingRevs
    let runPresent = mapConcurrently processDoc $ map revDocId presentRevs
    let runNextPage = processNext $ Couch.maxKey firstPage
    _ <- concurrently runPresent runNextPage
    return ()
  where
    processDoc = processDocument couch psql db
    checkRevs = checkRevisions psql db
    processNext (Just _)@key = processDatabasePage couch psql db $ Couch.getPage couch db key
    processNext None = ()

checkRevisions :: PsqlClient -> DbName -> DbPage -> IO (DocId, DocId)
-- ^Determines which document ids are not up to the most recent revisions, and which have the most recent revisions.
-- The left/'fst' list of tuple are the missing ids, and the right/'snd' list of the tuple are the found ids.
checkRevisions psql db page = do
    checked <- mapConcurrently check revList
    let checkedPairs = zip checked revList
    let (presentPairs, missingPairs) = partition fst checkedPairs
    let present = map snd presentPairs
    let missing = map snd missingPairs
    return (missing, present)
  where
    revList = Couch.toDocRevList page
    check = Psql.checkRevisionExists psql db

processDocument :: CouchClient -> PsqlClient -> DbName -> DocId -> IO ()
-- ^Responsible for processing a given couch document, including all of its revisions and attachments
processDocument couch psql db doc = do
    (_, details) <- concurrently ensureDoc fetchDoc
    Psql.withTransaction psql $ \transClient ->
      let processRev = proccessRevision couch transClient db
      let processAtt = processAttachment couch transClient db doc
      processRev $ Couch.currentRevInfo details -- Get at least the current revision in the DB ASAP
      _ <- mapConcurrently processRev $ docRevsInfo details
      _ <- mapConcurrently processAtt $ docAttachmentStubs details
      return ()
  where
    ensureDoc = Psql.ensureDocument psql db doc
    fetchDoc = Couch.getDocDetails db doc

processRevision :: CouchClient -> PsqlClient -> DbName -> DocRevInfo -> IO ()
processRevision couch psql db docRev  = do
    ensureRev
    processDetails $ revStatus docRev
    return ()
  where
    doc = docRevSpec docRev
    ensureRev = Psql.ensureRevision psql db doc
    getDetails = Couch.getRevDetails psql db doc
    procRevDets = processRevisionDetails psql db doc
    processDetails Couch.RevAvailable = getDetails >>= Psql.ensureRevisionContent psql db doc details
    processDetails _ = Psql.noteDeletedRevision psql db doc

processAttachment :: CouchClient -> PsqlClient -> DbName -> DocId -> AttachmentStub -> IO ()
processAttachment couch psql db doc att = do
    (_, content) <- concurrently ensureAtt fetchContent
    ensureAttContent content
    return ()
  where
    ensureAtt = Psql.ensureAttachment psql db doc att
    fetchContent = Couch.fetchAttachment db doc att
    ensureAttContent = Psql.ensureAttachmentContent psql db doc att
