{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Main where

import qualified Couch
import Couch ( DocId, DbName, DbPage, DocRevInfo, AttachmentStub, docId )
import qualified Psql
import qualified Config
import Data.Maybe ( Maybe(..) )
import Control.Concurrent.Async
import Data.List ( partition )

type CouchClient = Couch.Client
type PsqlClient = Psql.Client

-- TODO Attach EKG

main :: IO ()
main = do
    config <- Config.readConfig
    psql <- Psql.client config
    couch <- Couch.client config
    dbs <- Couch.getAllDbs couch
    let bgPoll = mapConcurrently (upDoc config couch psql) dbs
    let upDb = processDatabase couch psql
    let upDbs = mapConcurrently upDb dbs
    _ <- concurrently bgPoll upDbs
    return ()
  where
    upDoc config couch psql db = Couch.pollChanges config db $ processDocument couch psql db

processDatabase :: CouchClient -> PsqlClient -> DbName -> IO ()
-- ^Responsible for synchronizing a specific revision
processDatabase couch psql db = do
    Psql.ensureDatabase psql db
    firstPage <- getFirstPage
    processDatabasePage couch psql db firstPage
    return ()
  where
    getFirstPage = Couch.getPage couch db Nothing

processDatabasePage :: CouchClient -> PsqlClient -> DbName -> DbPage -> IO ()
-- ^Responsible for processing a given database page, including recursing into subsequent pages
processDatabasePage couch psql db page = do
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
    (_, details) <- concurrently ensureDoc fetchDoc
    Psql.withTransaction psql $ \transClient -> do
      let processRev = processRevision couch transClient db
      let processAtt = processAttachment couch transClient db doc
      _ <- mapConcurrently processRev $ Couch.docRevsInfo details
      _ <- mapConcurrently processAtt $ Couch.docAttachmentsList details
      return ()
  where
    ensureDoc = Psql.ensureDocument psql db doc
    fetchDoc = Couch.getDocDetails couch db doc

processRevision :: CouchClient -> PsqlClient -> DbName -> DocRevInfo -> IO ()
processRevision couch psql db docRev  = do
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
    (_, content) <- concurrently ensureAtt fetchContent
    ensureAttContent content
    return ()
  where
    ensureAtt = Psql.ensureAttachment psql db doc att
    fetchContent = Couch.fetchAttachment couch db doc att
    ensureAttContent = Psql.ensureAttachmentContent psql db doc att
