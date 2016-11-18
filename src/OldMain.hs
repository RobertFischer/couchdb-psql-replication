{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Main where

import Couch
import Psql

type JsonValue = Aeson.Value
type CouchManager = Manager
type HttpRequest = HTTP.Request
type HttpResponse = HTTP.Response ByteString
type CouchCallback = HttpRequest -> IO HttpResponse
type WithCouch = CouchCallback
type CouchDoc = String
type PsqlPool = Pool Connection
type PsqlCallback a = Connection -> IO a
type WithPsql = forall a. PsqlCallback a -> IO a
type DocContents = JsonValue
type SqlDefinition = String
type Unique = Bool
type ColumnsSql = String
type DocKey = String

newtype SchemaName = SchemaName String;
newtype TableName = TableName
  {
    tableSchema :: SchemaName,
    tableName :: String
  }
newtype IndexName = IndexName
  {
    indexTable :: TableName,
    indexName :: String
  }


newtype FullDocRev = FullDocRev
  {
    frevRev     :: DocRev,
    frevDoc     :: FullDoc,
    frevDetails :: Maybe DocRevDetails
    -- TODO Add in details from JSON
  }

newtype DbPage = DbPage
  {
    dbPageDb :: DbName,
    dbPageRevs :: [DocRev],
    maxKey :: Maybe DocKey
  }

newtype ServerAddress = ServerAddress
  {
    hostName :: String,
    hostPort :: Int
  }

newtype BackupConfig = BackupConfig
  {
    dbTableName             :: TableName,
    historyTableName        :: TableName,
    bodyTableName           :: TableName
    attachmentsTableName    :: TableName,
    contentTableName        :: TableName,
    deletedTableName        :: TableName,
    currentViewName         :: TableName,
    docAttachmentsViewName  :: TableName,
    couchAddress            :: ServerAddress,
    psqlAddress             :: ServerAddress
    psqlUsername            :: String,
    psqlPassword            :: String,
    psqlDatabase            :: String,
    psqlConcurrency         :: Int,
    couchConcurrency        :: Int
  }

type BackupM m a = ReaderT BackupConfig m a

class HasBackupConfig m where
  getBackupConfig :: m BackupConfig

instance {-# OVERLAPPING #-} HasBackupConfig BackupM where
  getBackupConfig = ask

instance {-# OVERLAPPABLE #-} (HasBackupConfig m, Trans t) => HasBackupConfig (t m) where
  getBackupConfig = lift . getBackupConfig

type WithPsqlM a = ReaderT WithPsql m a

class HasPsql m where
  withPsql :: PsqlCallback a -> IO a

instance {-# OVERLAPPING #-} HasPsql WithPsqlM where
  withPsql = ask

instance {-# OVERLAPPABLE #-} (HasPsql m, Trans t) => HasPsql (t m) where
  withPsql = lift . withPsql

type WithCouchM a = ReaderT WithCouch m a
class HasCouch m where
  withCouch :: WithCouch

instance {-# OVERLAPPING #-} HasCouch WithCouch where
  withCouch = ask

instance {-# OVERLAPPABLE #-} (HasCouch m, Trans t) => HasCouch (t m) where
  withCouch = lift . withCouch

type Url = String
type Result = JsonValue

class ToSql x where
  toSql :: x -> String

instance ToSql SchemaName where
  toSql (SchemaName(name)) = name

instance ToSql TableName where
  toSql tbl = (toSql $ tableSchema tbl) ++ "." ++ (tableName tbl)

instance ToSql IndexName where
  toSql idx = "idx" ++ (conventionalize name)
    where
      name = (toSql $ indexTable idx) ++ "Table." ++ (indexName idx)
      cap [] = []
      cap (x:xs) = (Char.toUpper x) : xs
      conventionalize []   = []
      conventionalize x:xs = cap $ conv' x xs
      checkX = Char.isAlphaNum
      conv' x []     | checkX x  = [x]
                     | otherwise = []
      conv' x (y:ys) | checkX x  = x:    (conv' y ys)
                     | otherwise = cap $ (conv' y ys)

flatten = undefined
inReaderT = flip runReaderT

newtype DocSummary = DocSummary {
  dsId :: String,
  dsKey :: String,
  dsValue :: Map String String
}
dsRev :: DocSummary -> String
dsRev ds = getRev
  where
    getRev = case maybeRev of None -> "Could not retrieve the revision from " ++ ds
                              Some x -> x
    maybeRev = lookup "rev" value
    value = dsValue ds

doAsync :: IO a -> (a -> IO b) -> IO b
doAsync action mapF  = withAsync action $ \promise -> do
  value <- wait promise
  mapF value

main :: IO ()
main = do
  _ <- forkServer "0.0.0.0" 8411
  config <- initBackupConfig
  let makeWithPsql = initWithPsql config
  let makeCouchCall = initCouch config
  (withPsql, couchCall) <- concurrently makeWithPsql makeCouchCall
  dbs <- fetchAllDbs config couchCall
  let mapDb = processDb config withPsql couchCall
  _ <- mapConcurrently mapDb dbs
  return ()

initBackupConfig :: () -> IO BackupCofig
initBackupConfig = do
    env <- getEnvironment
    let readEnv = readFromEnv env
    let couchAddr = readCouchAddress readEnv
    let sqlAddr = readPsqlAddress readEnv
    let schema = SchemaName $ readEnv "PSQL_SCHEMA" "couch"
    let table = TableName schema $ readEnv
    return BackupConfig {
      dbTableName = table "PSQL_TABLE_DBS" "documentDatabases"
      historyTableName = table "PSQL_TABLE_HISTORY" "documentHistory"
      bodyTableName = table "PSQL_TABLE_BODY" "documentContents"
      attachmentsTableName = table "PSQL_TABLE_DOCATTS" "documentAttachments"
      contentTableName = table "PSQL_TABLE_ATTCONT" "attachmentContent"
      deletedTableName = table "PSQL_TABLE_DELETED" "deletedDocuments"
      currentViewName = table "PSQL_VIEW_CURRENT"  "currentDocuments"
      docAttachmentsViewName = table "PSQL_VIEW_DOCATTS"  "attachments"
      couchAddress = couchAddr
      psqlAddress = sqlAddr
      psqlUsername = readEnv "PSQL_USERNAME" "postgres"
      psqlPassword = readEnv "PSQL_PASSWORD" ""
      psqlDatabase = readEnv "PSQL_DATABASE" "postgres"
      psqlConcurrency = read (readEnv "PSQL_CONCURRENCY" "100") :: Int
      couchConcurrency = read (readEnv "COUCH_CONCURRENCY" "20")  :: Int
    }
  where
    readFromEnv ((key,val):rest) var def | key == var = val
                                         | otherwise = readFromEnv rest var def
    readFromEnv [] _ def = def

readCouchAddress :: (String -> String -> String) -> ServerAddress
readCouchAddress readEnv =
  ServerAddress <$>
    pure (readEnv "COUCHDB_HOST" "localhost") <*>
    pure (read (readEnv "COUCHDB_PORT" "5984") :: Int)

readPsqlAddress :: (String -> String -> String) -> ServerAddress
readPsqlAddress readEnv =
  ServerAddress <$>
    pure (readEnv "PSQL_HOST" "localhost") <*>
    pure (read (readEnv "PSQL_PORT" "5432") :: Int)

initCouch :: (MonadIO m, HasBackupConfig m) => m WithCouch
initCouch = do
  config <- getBackupConfig
  let addr = couchAddress config
  let settings = HTTP.defaultManagerSettings {
    HTTP.managerModifyRequest = \req -> return req {
      method = methodGet,
      port = hostPort addr,
      host = hostName addr,
      decompress = const True,
      cookieJar = None
    }
  }
  manager <- liftIO $ HTTP.newManager settings
  let couchCall = (flip HTTP.httpLbs) manager
  inReaderT couchCall $ do
    checkCouchConnection
  let getCouch = return couchCall
  let killCouch = const $ return ()
  let concurrency = couchConcurrency config
  pool <- liftIO $ createPool getCouch killCouch 1 600 concurrency
  return $ \req -> withResource pool $ \call -> call req

checkCouchConnection:: CouchCallback -> IO ()
checkCouchConnection couchCall = do
    response <- couchCall request
    let status = responseStatus response
    case HTTPStatus.statusIsSuccessful status of
      True  -> return ()
      False -> error $ "Unsuccessful return from Couch: " ++ (show status)
  where
    request = couchRequest "/" Map.empty

couchRequest :: URLShow a => String -> Map a a -> HttpRequest
couchRequest reqPath reqQuery =
    defaultRequest {
      path = reqPath,
      query = foldr concatQuery "" $ toList reqQuery
    }
  where
    concatQuery :: URLShow a => String -> (a,a) -> String
    concatQuery ""   (key, val) = (urlShow key) ++ "=" ++ (urlShow val)
    concatQuery memo  term      = memo ++ "&" ++ ( concatQuery "" term )

initWithPsql :: (MonadIO m, HasBackupConfig m) => m WithPsql
initWithPsql = do
    config <- getBackupConfig
    let psqlAddr = psqlAddress config
    let connect = Psql.connect $ Psql.defaultConnectInfo {
      Psql.connectHost = hostName psqlAddr,
      Psql.connectPort = hostPort psqlAddr,
      Psql.connectUser = psqlUsername config,
      Psql.connectPassword = psqlPassword config,
      Psql.connectDatabase = psqlDatabase config
    }
    let concurrency = couchConcurrency config
    pool <- createPool connect close 1 0.5 concurrency
    let withPsql = withResource pool
    ensurePsqlStructure config withPsql
    return withPsql
  where
    close = Psql.close

ensurePsqlStructure :: BackupConfig -> WithPsql -> IO ()
ensurePsqlStructure config withPsql = do
    -- Change control is by executing things in rounds
    _ <- mapConcurrently $ applyArgs round0
    _ <- mapConcurrently $ applyArgs round1
    _ <- mapConcurrently $ applyArgs round2
    _ <- mapConcurrently $ applyArgs round3
  where
    applyArgs = map $ \f -> f config withPsql
    round0 = [ ensureDbTable ]
    round1 = [ ensureHistoryTable, ensureContentsTable ]
    round2 = [ ensureCurrentView, ensureAttachmentsTable, ensureBodyTable ]
    round3 = [ ensureDocAttachmentsView ]

ensureDbTable :: BackupConfig -> WithPsql -> IO ()
ensureDbTable config withPsql = do
    executeSql withPsql def
  where
    tbl = dbTableName config
    tblSql = toSql tbl
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "name VARCHAR UNIQUE NOT NULL " ++
      ")"

ensureBodyTable :: BackupConfig -> WithPsql -> IO ()
ensureBodyTable config withPsql = do
    executeSql withPsql def
  where
    tblSql = toSql $ bodyTableName config
    historyTableSql = toSql $ historyTableName config
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "createdAt TIMESTAMPZ NOT NULL DEFAULT NOW(), " ++
      "historyId BIGINT NOT NULL REFERENCES " ++ historyTableSql ++ "(id) ON DELETE RESTRICT ON UPDATE CASCADE, " ++
      "data JSONB NOT NULL " ++
      ")"

ensureHistoryTable :: BackupConfig -> WithPsql -> IO ()
ensureHistoryTable config withPsql = do
    executeSql withPsql def
    _ <- mapConcurrently [
      ensureIndex withPsql (IndexName tbl "docRevIdentifier") True  "dbId, docId, revId",
      ensureIndex withPsql (IndexName tbl "docRevOrd")        False "dbId, docId, revOrd",
      ensureIndex withPsql (IndexName tbl "deletedAt")        False "deletedAt"
    ]
  where
    dbTblSql = toSql $ dbTableName config
    tbl = historyTableName config
    tblSql = toSql tbl
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "dbId BIGINT NOT NULL REFERENCES " ++ dbTblSql ++ "(id) ON DELETE RESTRICT ON UPDATE CASCADE, " ++
      "docId VARCHAR NOT NULL, " ++
      "revId VARCHAR NOT NULL, " ++
      "revOrd SMALLINT NULL DEFAULT NULL, " ++
      [sql|
      firstSeenAt TIMESTAMPZ NOT NULL DEFAULT NOW(),
      lastSeenAt TIMESTAMPZ NOT NULL DEFAULT NOW(),
      deletedAt TIMESTAMPZ NULL DEFAULT NULL
      )
      |]

ensureContentsTable :: BackupConfig -> WithPsql -> IO ()
ensureContentsTable config withPsql = do
    executeSql withPsql def
  where
    tbl = contentsTableName config
    tblSql = toSql tbl
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "createdAt TIMESTAMPZ NOT NULL DEFAULT NOW(), " ++
      "data BYTEA NOT NULL UNIQUE " ++
      ")"

ensureAttachmentsTable :: BackupConfig -> WithPsql -> IO ()
ensureAttachmentsTable config withPsql = do
    executeSql withPsql def
    ensureIndex withPsql (IndexName tbl "revIdName") True  "revId, name"
  where
    tbl = attachmentsTableName config
    tblSql = toSql tbl
    historyTblSql = toSql $ historyTableName config
    contentTblSql = toSql $ contentTableName config
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "name VARCHAR NOT NULL, " ++
      "contentType VARCHAR NOT NULL, " ++
      "digest VARCHAR NOT NULL, " ++
      "revId BIGINT NOT NULL REFERENCES " ++ historyTblSql ++ "(id) ON DELETE RESTRICT ON UPDATE CASCADE, " ++
      "contentId BIGINT NOT NULL REFERENCES " ++ contentTblSql ++ "(id) ON DELETE RESTRICT ON UPDATE CASCADE " ++
      ")"

ensureCurrentView :: BackupConfig -> WithPsql -> IO ()
ensureCurrentView config withPsql = do
    executeSql withPsql def
  where
    tbl = currentViewName config
    tblSql = toSql tbl
    historyTblSql = toSql $ historyTableName config
    dbTblSql = toSql $ dbTableName config
    bodyTblSql = toSql $ bodyTableName config
    def =
      "CREATE OR REPLACE VIEW " ++ tbSql ++ " AS " ++
      "SELECT DISTINCT ON (h.dbId, h.docId) " ++
      "h.id AS id, h.dbId AS dbId, d.name AS name, h.docId AS docId, " ++
      "h.firstSeenAt AS firstSeenAt, h.lastSeenAt AS lastSeenAt, h.content AS content" ++
      "FROM " ++ historyTblSql ++ " h " ++
      "INNER JOIN " ++ dbTblSql ++ " d ON (d.id = h.dbId) " ++
      "INNER JOIN " ++ bodyTblSql ++ " b ON (b.historyId = h.id) " ++
      "WHERE " ++
        " h.deletedAt IS NULL, " ++
        " h.revId IS NOT NULL " ++
      "ORDER BY h.dbId ASC, h.docId ASC, h.revOrd DESC, h.revId DESC "

ensureDocAttachmentsView :: BackupConfig -> WithPsql -> IO ()
ensureDocAttachmentsView config withPsq = do
    executeSql withPsql def
  where
    tbl = docAttachmentsViewName config
    tblSql = toSql tbl
    historyTblSql = toSql $ historyTableName config
    attachmentsTblSql = toSql $ attachmentsTableName config
    currentViewSql = toSql $ currentViewName config
    def =
      "CREATE OR REPLACE VIEW " ++ tblSql ++ " AS " ++
      "SELECT DISTINCT ON (h.docId, a.name) " ++
      "a.id AS id, h.docId AS docId, a.name AS name, a.contentType AS contentType, a.digest AS digest, d.data AS data " ++
      "FROM " ++ historyTblSql + " h " ++
      "INNER JOIN " ++ attachmentsTblSql ++ " a ON (h.id = a.revId) " ++
      "INNER JOIN " ++ contentsTblSql ++ " d ON (a.contentId = d.id) " ++
      "INNER JOIN " ++ currentViewSql ++ " c ON (c.docId = h.docId) " ++
      "ORDER BY h.docId DESC, h.revOrd DESC, a.name DESC"

querySql :: ToRow q => WithPsql -> Query -> q -> IO ()
querySql withPsql query row = withPsql $ \conn -> do
  _ <- query conn query q
  return ()

executeSql :: WithPsql -> Query -> IO ()
executeSql withPsql query = withPsql $ \conn -> do
  _ <- execute_ conn query
  return ()

ensureIndex :: WithPsql -> IndexName -> Unique -> ColumnsSql -> IO ()
ensureIndex withPsql idx uniq cols = withPsql $ \conn -> do
    executeSql_ conn def
    return ()
  where
    uniqueStr = case uniq of
                  True => " UNIQUE "
                  False => ""
    idxName = toSql idx
    tblName = toSql $ indexTable idx
    def =
      "CREATE " ++ uniqueStr ++ " INDEX IF NOT EXISTS " ++
      idxName ++ " ON " ++ tblName ++ " ( " ++ cols ++ " )"

fetchAllDbs :: BackupConfig -> CouchCall -> IO [DbName]
fetchAllDbs config couchCall = do
    dbsStr <- responseBody $ couchCall $ couchRequest "/_all_dbs" Map.empty
    return $ map DbName $ parseDbs dbsStr
  where
    parseDbs :: String -> List String
    parseDbs str = case decode str of None   -> error ("Could not decode DBs from " ++ str)
                                      Some x -> x

processDb :: BackupConfig -> WithPsql -> CouchCall -> DbName -> IO ()
processDb config withPsql couchCall db = do
    page <- getFirstPage couchCall db
    processDbPage config withPsql couchCall page

processDbPage :: BackupConfig -> WithPsql -> CouchCall -> DbPage -> IO ()
processDbPage config withPsql couchCall page = do
    (missingRevs, presentRevs) <- checkRevs config withPsql $ dbPageRevs revs
    _ <- mapConcurrently doProcessRev missingRevs -- Do the missing revisions first
    -- Now do the present revisions (if any) at the same time we are doing the next page (if any)
    let processPresent = mapConcurrently doProcessRev presentRevs
    case maxKey page of None -> processPresent
                        Some(key) -> withAwait processPresent $ \promise -> do
                          doProcessDbAfter key
                          _ <- wait promise
                          return ()
  where
    dbName = dbPageDb page
    doProcessRev rev = processDocId config withPsql couchCall $ revDocId rev
    doProcessDbAfter = processDbAfter config withPsql couchCall dbName

checkRevs :: BackupConfig -> WithPsql -> [DocRev] -> IO ([DocRev], [DocRev])
checkRevs config withPsql revs = do
    revCheckResult <- mapConcurrently doCheckRev revs
    let missing = mapCompact leftToMaybe revCheckResult
    let found = mapCompact rightToMaybe revCheckResult
    return (missing, found)
  where
    mapCompact f lst = catMaybes $ map f lst
    doCheckRev = checkRev config withPsql

checkRev :: BackupConfig -> WithPsql -> DocRev -> Either DocRev DocRev
checkRev config withPsql rev = do
  found <- queryRevExistence config withPsql rev
  return case found of False -> Left  rev
                  True  -> Right rev

queryRevExistence :: BackupConfig -> WithPsql -> DocRev -> Boolean
queryRevExistence config withPsql rev = do
  undefined

processDbAfter :: BackupConfig -> WithPsql -> CouchCall -> DbName -> DocKey -> IO ()
processDbAfter config withPsql couchCall dbName docKey = do
  page <- getLaterPage couchCall dbName
  processDbPage config withPsql couchCall page

processDocId :: BackupConfig -> WithPsql -> CouchCall -> DocId -> IO ()
processDocId config withPsql couchCall docId = do
    doc <- fetchDoc couchCall docId
    processDoc config withPsql couchCall doc
  where
    dbName = docDb docId

fetchDoc :: CouchCall -> DocId -> IO FullDoc
fetchDoc couchCall docId = do
  undefined

processDoc :: BackupConfig -> WithPsql -> CouchCall -> FullDoc -> IO ()
processDoc config withPsql couchCall doc = do
    _ <- mapConcurrently doProcessRev $ fullRevs
  where
    revs = fullDocRevIds doc
    doProcessRev docRev = do
      fullRev <- getFullRev doc docRev
      processDocRev config withPsql fullRev

getFullRev :: CouchCall -> DocRev -> IO FullDocRev
getFullRev couchCall rev = do
  undefined

processDocRev :: BackupConfig -> WithPsql -> CouchCall -> FullDocRev -> IO ()
processDocRev config withPsql couchCall rev = do
  where


getFirstPage :: CouchCall -> DbName -> IO DbPage
getFirstPage couchCall dbName = do
  undefined

getPageAfter :: CouchCall -> DbName -> DocKey -> IO DbPage
getPageAfter couchCall dbName docRev = do
  undefined

getPage :: CouchCall -> DbName -> Map String String -> IO DbPage
getPage couchCall DbName(db) queryParams = do
    undefined
  where
    url = "/" ++ db ++ "/_all_docs"
