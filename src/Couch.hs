{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types #-}

module Couch where

import Config
import Control.Applicative ( (<|>) )
import Control.Concurrent ( yield )
import Control.Concurrent.Async
import Data.Aeson ( parseJSON, FromJSON, (.:), (.!=), (.:?) )
import Data.Aeson.Types ( typeMismatch, Parser )
import Data.ByteString.Lazy hiding (map, split, null)
import Data.CaseInsensitive as CI
import Data.Foldable ( maximumBy )
import Data.Maybe (fromMaybe)
import Data.Ord ( comparing, Ordering )
import Data.Pool
import GHC.Generics (Generic)
import Network.HTTP.Simple
import Network.HTTP.Types (HeaderName)
import qualified Data.Aeson as Json
import qualified Data.ByteString.Char8 as B8
import qualified Data.Char as Char
import qualified Data.HashMap.Strict as Map
import qualified Data.Text as Text
import qualified Network.HTTP.Client as HTTP
import Util

removeQuotesToString :: Text.Text -> String
removeQuotesToString = removeQuotes . Text.unpack

type HashMap = Map.HashMap
type HttpRequest = HTTP.Request

data Client = Client
  {
    fetchBS :: HttpRequest -> IO (HTTP.Response ByteString),
    fetchJSON :: forall a. FromJSON a => HttpRequest -> IO (HTTP.Response a)
  }
type CouchClient = Client

-- |Represents the name of a CouchDB database
newtype DbName = DbName String deriving (Generic, Read, Show, Eq)
instance FromJSON DbName where
  parseJSON (Json.String s) = DbName . removeQuotesToString <$> pure s
  parseJSON invalid        = typeMismatch "Database Name" invalid

dbNameStr :: DbName -> String
-- ^Provides the database name as a String
dbNameStr (DbName a) = a

-- |Represents the ID of a document from CouchDB
newtype DocId = DocId String deriving (Generic, Read, Show, Eq)
instance FromJSON DocId where
  parseJSON (Json.String s) = DocId . removeQuotesToString <$> pure s
  parseJSON (Json.Object o) = DocId . removeQuotesToString <$> ( ( o .: "_id" <|> o .: "id" ) :: Parser Text.Text )
  parseJSON invalid = typeMismatch "Document ID" invalid

docIdStr :: DocId -> String
-- ^Provides the document id as a String
docIdStr (DocId d) = d

-- |Represents the revision of a document
data DocRev = DocRev
  {
    docId :: DocId,
    docRevId :: RevId
  } deriving (Generic, Read, Show, Eq)

instance FromJSON DocRev where
  parseJSON (Json.Object o) = DocRev <$>
    ( o .: "_id" <|> o .: "id" ) <*>
    ( o .: "_rev" <|> o .: "rev" <|> (o .: "value" >>= \val -> val .: "rev") )
  parseJSON invalid = typeMismatch "Document Revision" invalid

-- |Represents the ID of a revision of a document from CouchDB
data RevId = RevId
  {
    revOrd :: Int,
    revId :: String
  } deriving (Generic, Read, Show, Eq)

instance FromJSON RevId where
  parseJSON (Json.String s) = revFromFullId . removeQuotesToString <$> pure s
  parseJSON (Json.Object o) = revFromFullId . removeQuotesToString <$> ( ( o .: "_rev" <|> o .: "rev" ) :: Parser Text.Text )
  parseJSON invalid = typeMismatch "Revision ID" invalid

revFullId :: RevId -> String
-- ^Provides the "full id" (consisting of the ordinal, a dash, then the id) of the document revision
revFullId rev = ord ++ "-" ++ rid
  where
    ord = show . revOrd $ rev
    rid = revId rev

revFromFullId :: String -> RevId
-- ^Given a id and the full revision specification (ordinal, a dash, id), generates the document revision structure
revFromFullId fullRev = RevId
    {
      revOrd = ordFromFull,
      revId = idFromFull
    }
  where
    ordFromFull = read $ fst doSplit
    idFromFull = snd doSplit
    doSplit = split [] $ removeQuotes fullRev
    split :: String -> String -> (String, String)
    split _ [] = error ("Could not split " ++ fullRev)
    split start (x:xs)
        | x == '-' = (start, xs)
        | otherwise = (start ++ (x : (fst split')), snd split')
      where
        split' = split [] xs

-- |Represents the possible statuses of a revision
data RevStatus = RevDeleted | RevMissing | RevAvailable deriving (Show, Read, Eq)

instance FromJSON RevStatus where
  parseJSON value@(Json.String txt ) =
      f . removeQuotes $ str
    where
      str = Text.unpack txt
      f :: String -> Parser RevStatus
      f [] = typeMismatch ("Empty string given for revision status") value
      f (c:cs)
        | c == 'a' || c == 'A' = pure RevAvailable
        | c == 'm' || c == 'M' = pure RevMissing
        | c == 'd' || c == 'D' = pure RevDeleted
        | c == '"'          = f cs
        | Char.isSpace c   = f cs
        | otherwise         = typeMismatch ("revision status from string: " ++ str) value
  parseJSON invalid     = typeMismatch "revision status" invalid

data AttachmentStub = AttachmentStub
  {
    attName :: String,
    attContentType :: String,
    attFullDigest :: String,
    attLength :: Integer,
    attRevPos :: Int
  } deriving (Show, Read, Eq, Generic)

attDigest :: AttachmentStub -> String
-- ^Gives just the value of the digest (not the type of the digest, which is prepended
attDigest stub = afterDash digStr
  where
    digStr = removeQuotes . attFullDigest $ stub
    afterDash [] = error $ "Could not find the digest portion of " ++ digStr
    afterDash (x:xs)
      | x == '-' = xs
      | otherwise = afterDash xs

newtype AttachmentStubs = AttachmentStubs [AttachmentStub] deriving (Generic, Show, Read, Eq)
docAttachmentStubsAsList :: AttachmentStubs -> [AttachmentStub]
docAttachmentStubsAsList (AttachmentStubs stubs) = stubs

instance FromJSON AttachmentStubs where
  parseJSON (Json.Object o) =
      AttachmentStubs <$> ( Prelude.foldr f (pure []) $ Map.toList o )
    where
      f :: (Text.Text,Json.Value) -> Parser [AttachmentStub] -> Parser [AttachmentStub]
      f (name, Json.Object val) parsers = do
        stubs <- parsers
        stub <- makeStub
        return (stub : stubs)
        where
          makeStub = AttachmentStub <$>
            (pure . Text.unpack $ name ) <*>
            val .: "content_type" <*>
            val .: "digest" <*>
            val .: "length" <*>
            val .: "revpos"
      f (key, badVal) _ = typeMismatch ("Could not read attachment stub for key " ++ (Text.unpack key)) badVal
  parseJSON invalid = typeMismatch "Could not read attachment stubs"  invalid

-- |Represents the details of a document
data DocDetails = DocDetails
  {
    docDetailsDeleted :: Bool, -- Whether the document was deleted
    docAttachmentStubs :: AttachmentStubs, -- The details about attachments
    docRevsInfo :: [DocRevInfo], -- The details about revivsions
    docContent :: Json.Object -- Full content of the document
  } deriving (Generic, Show, Read, Eq)

docAttachmentsList :: DocDetails -> [AttachmentStub]
docAttachmentsList = docAttachmentStubsAsList . docAttachmentStubs

instance FromJSON DocDetails where
  parseJSON (Json.Object o) = DocDetails <$>
      (o .:? "_deleted" .!= False ) <*>
      (o .:? "_attachments" .!= AttachmentStubs [] ) <*>
      (o .:? "_revs_info" .!= [] ) <*>
      pure o
  parseJSON invalid = typeMismatch "document details" invalid

currentRevInfo :: DocDetails -> DocRevInfo
currentRevInfo details = maximumBy cmp revs
  where
    revs :: [DocRevInfo]
    revs = docRevsInfo details
    cmp :: DocRevInfo -> DocRevInfo -> Ordering
    cmp = comparing $ revOrd . docRevId . docRevSpec

-- |Represents the information about a given document
data DocRevInfo = DocRevInfo
  {
    docRevSpec :: DocRev,
    docRevStatus :: RevStatus
  } deriving (Generic, Show, Read, Eq)

instance FromJSON DocRevInfo where
  parseJSON (Json.Object o) = DocRevInfo <$>
    o .: "rev" <*>
    o .: "status"
  parseJSON invalid = typeMismatch "document revision info" invalid

-- |Represents a document with both an id and its content details
data FullDoc = FullDoc
  {
    fdocId :: DocId,
    fdocDetails :: DocDetails
  } deriving (Generic, Show, Read, Eq)

instance FromJSON FullDoc where
  parseJSON obj@(Json.Object _) = FullDoc <$>
    parseJSON obj <*>
    parseJSON obj
  parseJSON invalid = typeMismatch "full document" invalid

newtype DbPageKey = DbPageKey String deriving (Generic, Show, Read, Eq)

instance FromJSON DbPageKey where
  parseJSON (Json.Object o) = DbPageKey <$> o .: "key"
  parseJSON (Json.String s) = pure $ (DbPageKey . removeQuotesToString) s
  parseJSON invalid = typeMismatch "document key in database page" invalid

-- |A page of results from querying the database
data DbPage = DbPage
  {
    dbPageKeys :: [DbPageKey],
    dbPageDocRevs :: [DocRev]
  } deriving (Generic, Show, Read, Eq)

instance FromJSON DbPage where
  parseJSON (Json.Object o) = DbPage <$>
      keys <*>
      rows
    where
      rows = o .: "rows"
      keys = o .: "rows"
  parseJSON invalid = typeMismatch "database page of documents" invalid

toDocRevList :: DbPage -> [DocRev]
toDocRevList = dbPageDocRevs

toPageKeyList :: DbPage -> [DbPageKey]
toPageKeyList = dbPageKeys

data ChangeSet = ChangeSet
  {
    lastSeq :: Maybe String,
    docChanges :: [DocChange]
  }

instance FromJSON ChangeSet where
  parseJSON (Json.Object o) = ChangeSet <$>
      maybeLastSeq <*>
      ( o .:? "results" .!= [] )
    where
      maybeLastSeq :: Parser (Maybe String)
      maybeLastSeq = do
        result <- (o .:? "last_seq" .!= Nothing) :: Parser (Maybe Integer)
        return $ show <$> result
  parseJSON invalid = typeMismatch "database changeset" invalid

data DocChange = DocChange
  {
    changedDocId :: DocId,
    changeRevIds :: [RevId],
    docDeleted :: Bool
  }

instance FromJSON DocChange where
  parseJSON (Json.Object o) = DocChange <$>
    ( o .: "id" ) <*>
    ( o .: "changes" ) <*>
    ( o .:? "deleted" .!= False )
  parseJSON invalid = typeMismatch "document changeset" invalid


makeClient :: ReplConfig -> IO CouchClient
-- ^Creates a client matching the given replication configuration
makeClient config = do
  Config.logInfo $ "Creating Couch client"
  throttle <- createPool creator destroyer stripes timeoutSecs concurrency
  mngr <- HTTP.newManager $ HTTP.defaultManagerSettings { HTTP.managerConnCount = concurrency }
  return $ Client
    {
    fetchBS = couchRequest throttle mngr httpLBS,
    fetchJSON = couchRequest throttle mngr httpJSON
    }
  where
    couchRequest pool mngr reqFunc = \req ->
      withResource pool $ const $ do
        (reqFunc . mangleRequest mngr) req
    mangleRequest mngr = setRequestIgnoreStatus . (setMngr mngr) . setHost . setPort
    setMngr mngr = setRequestManager mngr
    setHost = setRequestHost $ couchHostBS config
    setPort = setRequestPort $ couchPort config
    timeoutSecs = 3600
    stripes = 1
    creator = return ()
    destroyer = const $ return ()
    concurrency = couchConcurrency config

pathToRequest :: String -> IO HttpRequest
-- ^Given a path, generate a GET Request.
pathToRequest ('/':path) = pathToRequest path
pathToRequest path = do
    yield
    Config.logInfo $ "Loading path:\t" ++ path
    HTTP.parseRequest fullPath
  where
    fullPath = "GET http://couch/" ++ path

pathArgsToObj :: FromJSON a => String -> [(String, String)] -> Client -> IO a
pathArgsToObj path args client = do
    req <- pathToRequest path
    let req' = setReqHeads req
    resp <- fetchJSON client req'
    return $ getResponseBody resp
  where
    setReqHeads = setRequestHeaders $ convertToHeaders args
    convertToHeaders :: [(String, String)] -> [(HeaderName, B8.ByteString)]
    convertToHeaders [] = []
    convertToHeaders ((a, b):xs) = (CI.mk $ B8.pack a, B8.pack b) : (convertToHeaders xs)

pathToObj :: FromJSON a => String -> Client -> IO a
pathToObj path client = pathArgsToObj path [] client

getAllDbs :: CouchClient -> IO [DbName]
-- ^Provides all the DB names, including system databases
getAllDbs client = do
  Config.logInfo "Fetching all databases"
  pathToObj "/_all_dbs" client

dbPagePath :: DbName -> String
dbPagePath (DbName db) = "/" ++ db ++ "/_all_docs"

getPage :: CouchClient -> DbName -> Maybe DbPageKey -> IO DbPage
-- ^Provides a page of documents from the database, optionally after some given key
getPage client db Nothing = do
  Config.logInfo $ "Fetching initial page from " ++ (show db)
  pathToObj (dbPagePath db) client
getPage client db (Just (DbPageKey key)) = do
  Config.logInfo $ "Fetching page from " ++ (show db) ++ " after " ++ key
  pathArgsToObj (dbPagePath db) [("startkey", key)] client

maxKey :: DbPage -> Maybe DbPageKey
-- ^Provides the maximum key for a page, or `None` if there are no records on the page.
maxKey (DbPage pages _)
  | null pages = Nothing
  | otherwise = Just $ maximumBy (comparing unpacked) pages
    where
      unpacked :: DbPageKey -> String
      unpacked (DbPageKey key) = key

pollChanges :: ReplConfig -> DbName -> (DbName -> DocChange -> IO ()) -> IO ()
-- ^Poll for changes in the given database, calling the callback function for each doc id which
-- is reported to have a change.
pollChanges origConfig db callback = do
    yield
    Config.logInfo $ "Polling for changes in " ++ (show db)
    couch <- makeClient config
    longpollDb couch db startSeq callback
  where
    startSeq = startingSequence config
    config = origConfig
      {
        couchConcurrency = 1,
        psqlConcurrency = 1
      }

longpollDb :: CouchClient -> DbName -> String -> (DbName -> DocChange -> IO ()) -> IO ()
longpollDb couch db since callback = do
    yield
    Config.logInfo $ "Beginning a long poll round for " ++ (show db)
    changes <- pathArgsToObj changesPath args couch
    _ <- mapConcurrently (callback db) $ docChanges changes
    let nextSeq = fromMaybe since $ lastSeq changes
    longpollDb couch db nextSeq callback
  where
    changesPath = "/" ++ (dbNameStr db) ++ "/_changes"
    args =
      [
        ("since", since),
        ("feed", "longpoll"),
        ("heartbeat", "1000")
      ]

getDocDetails :: CouchClient -> DbName -> DocId -> IO DocDetails
-- ^For a given document id, retrieve the document details.
getDocDetails couch db doc = do
    Config.logInfo $ "Retrieving doc details for " ++ (show db) ++ " " ++ (show doc)
    pathArgsToObj docDetailsPath args couch
  where
    docDetailsPath = "/" ++ (dbNameStr db) ++ "/" ++ (docIdStr doc)
    args = [ ("revs_info", "true") ]

getRevDetails :: CouchClient -> DbName -> DocRev -> IO DocDetails
-- ^For a given revision specification, retrieve the document details for that revision.
getRevDetails couch db rev = do
    Config.logInfo $ "Retrieving rev details for " ++ (show db) ++ " " ++ (show rev)
    pathArgsToObj revDetailsPath args couch
  where
    doc = docId rev
    revDetailsPath = "/" ++ (dbNameStr db) ++ "/" ++ (docIdStr doc)
    args = [ ("rev", revFullId . docRevId $ rev) ]

fetchAttachment :: CouchClient -> DbName -> DocId -> AttachmentStub -> IO ByteString
fetchAttachment couch db doc att = do
    Config.logInfo $ "Retrieving attachment for " ++ (show db) ++ " " ++ (show doc) ++ " " ++ (show att)
    req <- pathToRequest attPath
    responseBs <- fetchBS couch req
    return $ getResponseBody responseBs
  where
    attPath = "/" ++ (dbNameStr db) ++ "/" ++ (docIdStr doc) ++ "/" ++ (attName att)
