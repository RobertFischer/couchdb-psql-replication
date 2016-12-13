{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Main where

import Control.Concurrent ( yield )
import Control.Concurrent.Async
import Couch ( DocId, DbName, DbPage, DocRevInfo, AttachmentStub, docId )
import Data.List ( partition )
import Data.Maybe ( Maybe(..) )
import qualified Config
import qualified Couch
import qualified Psql
--import System.Remote.Monitoring as EKG
import System.Timeout

type CouchClient = Couch.Client
type PsqlClient = Psql.Client

main :: IO ()
main = do
    --_ <- EKG.forkServer "localhost" 41100
    Config.logInfo "Reading config and creating clients"
    config <- Config.readConfig
    psql <- Psql.makeClient config
    couch <- Couch.makeClient config
    Config.logInfo "Loading DBs starting polling"
    dbs <- Couch.getAllDbs couch
    let poller = mapConcurrently (upDoc config couch psql) dbs
    let upDb = processDatabase couch psql
    withAsync poller $ \pollPromise -> do
      Config.logInfo "Loading live revisions"
      liveRevs <- Psql.fetchLiveRevisions psql
      Config.logInfo "Processing live revisions"
      _ <- mapConcurrently (processRevisionRecord couch psql) liveRevs
      Config.logInfo "Processing databases"
      _ <- mapConcurrently upDb dbs
      let timeoutSecs = Config.pollLength config
      Config.logInfo $ "Waiting for changeset polling timeout:\t" ++ (show timeoutSecs) ++"s"
      _ <- timeout timeoutSecs $ wait pollPromise
      return ()
  where
    upDoc config couch psql = \db -> do
      yield
      Config.logInfo $ "Processing database: " ++ (show db)
      let procDoc = processDocumentChange couch psql
      Couch.pollChanges config db procDoc

processRevisionRecord :: CouchClient -> PsqlClient -> Psql.RevRecord -> IO ()
processRevisionRecord couch psql rev = do
    yield
    Config.logInfo $ "Processing revision:\t" ++ (show rev)
    processRevision couch psql db docRevInfo
  where
    db = Psql.revRecordDb rev
    docRevInfo = Couch.DocRevInfo
      {
        Couch.docRevSpec = docRev,
        Couch.docRevStatus = Couch.RevAvailable
      }
    docRev = Couch.DocRev
      {
        Couch.docId = Psql.revRecordDoc rev,
        Couch.docRevId = Psql.revRecordRev rev
      }

processDatabase :: CouchClient -> PsqlClient -> DbName -> IO ()
-- ^Responsible for synchronizing a specific revision
processDatabase couch psql db = do
    yield
    Config.logInfo $ "Processing database:\t" ++ (show db)
    Psql.ensureDatabase psql db
    firstPage <- getFirstPage
    processDatabasePage couch psql db firstPage
    return ()
  where
    getFirstPage = Couch.getPage couch db Nothing

processDatabasePage :: CouchClient -> PsqlClient -> DbName -> DbPage -> IO ()
-- ^Responsible for processing a given database page, including recursing into subsequent pages
processDatabasePage couch psql db page = do
    yield
    Config.logInfo $ "Processing database page:\t" ++ (show db) ++ "\t" ++ (show page)
    (missingIds, presentIds) <- checkRevs page
    _ <- mapConcurrently processDoc missingIds
    let runPresent = mapConcurrently processDoc presentIds
    let runNextPage = processNext $ Couch.maxKey page
    _ <- concurrently runPresent runNextPage
    return ()
  where
    processDoc = processDocument couch psql db
    checkRevs = checkRevisions psql db
    processNext key@(Just _) = do
      nextPage <- Couch.getPage couch db key
      processDatabasePage couch psql db nextPage
      return ()
    processNext Nothing = do
      return ()

checkRevisions :: PsqlClient -> DbName -> DbPage -> IO ([DocId], [DocId])
-- ^Determines which document ids are not up to the most recent revisions, and which have the most recent revisions.
-- The left/'fst' list of tuple are the missing ids, and the right/'snd' list of the tuple are the found ids.
checkRevisions psql db page = do
    yield
    checked <- mapConcurrently check revList
    let checkedPairs = zip checked revList
    let (presentPairs, missingPairs) = partition fst checkedPairs
    let mapSndDoc = map $ docId . snd
    let present = mapSndDoc presentPairs
    let missing = mapSndDoc missingPairs
    return (missing, present)
  where
    revList = Couch.toDocRevList page
    check = Psql.checkRevisionExists psql db

processDocument :: CouchClient -> PsqlClient -> DbName -> DocId -> IO ()
-- ^Responsible for processing a given couch document, including all of its revisions and attachments
processDocument couch psql db doc = do
    yield
    (_, details) <- concurrently ensureDoc fetchDoc
    Psql.withTransaction psql $ \transClient -> do
      let processRev = processRevision couch transClient db
      let processAtt = processAttachment couch transClient db doc
      _ <- mapConcurrently processRev $ Couch.docRevsInfo details
      _ <- mapConcurrently processAtt $ Couch.docAttachmentsList details
      if (Couch.docDetailsDeleted details) then (Psql.noteDeletedDocument psql db doc) else ( return () )
  where
    ensureDoc = Psql.ensureDocument psql db doc
    fetchDoc = Couch.getDocDetails couch db doc

processRevision :: CouchClient -> PsqlClient -> DbName -> DocRevInfo -> IO ()
processRevision couch psql db docRev  = do
    yield
    ensureRev
    processDetails $ Couch.docRevStatus docRev
    return ()
  where
    doc = Couch.docRevSpec docRev
    ensureRev = Psql.ensureRevision psql db doc
    getDetails = Couch.getRevDetails couch db doc
    processDetails Couch.RevAvailable = getDetails >>= \details -> Psql.ensureRevisionContent psql db doc details
    processDetails _ = Psql.noteDeletedRevision psql db doc

processAttachment :: CouchClient -> PsqlClient -> DbName -> DocId -> AttachmentStub -> IO ()
processAttachment couch psql db doc att = do
    yield
    (_, content) <- concurrently ensureAtt fetchContent
    ensureAttContent content
    return ()
  where
    ensureAtt = Psql.ensureAttachment psql db doc att
    fetchContent = Couch.fetchAttachment couch db doc att
    ensureAttContent = Psql.ensureAttachmentContent psql db doc att

processDocumentChange :: CouchClient -> PsqlClient -> DbName -> Couch.DocChange -> IO ()
processDocumentChange couch psql dbName change = do
    yield
    _ <- mapConcurrently procRev docRevInfos
    if isDeleted then Psql.noteDeletedDocument psql dbName doc else return ()
  where
    procRev = processRevision couch psql dbName
    docRevInfos = (flip map) (Couch.changeRevIds change) $ \rev -> Couch.DocRevInfo
      {
        Couch.docRevSpec = revSpec rev,
        Couch.docRevStatus = if isDeleted then Couch.RevDeleted else Couch.RevAvailable
      }
    revSpec rev = Couch.DocRev
      {
        Couch.docId = doc,
        Couch.docRevId = rev
      }
    doc = Couch.changedDocId change
    isDeleted = Couch.docDeleted change
