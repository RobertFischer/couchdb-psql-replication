{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Main where

import qualified Database.PostgreSQL.Simple as Psql
import qualified Data.Aeson as Aeson
import qualified Data.Char as Char
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Types.Status as HTTPStatus

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

newtype DbName = DbName String
newtype DocId = DocId
  {
    docDb :: DbName,
    docId :: String
  }
newtype DocRev = DocRev
  {
    revDocId :: DocId,
    revId :: String
  }

revDbName :: DocRev -> DbName
revDbName doc = docDb $ revDocId doc

newtype ServerAddress = ServerAddress
  {
    hostName :: String,
    hostPort :: Int
  }

newtype BackupConfig = BackupConfig
  {
    dbTableName          :: TableName,
    historyTableName     :: TableName,
    attachmentsTableName :: TableName,
    currentViewName      :: TableName,
    couchAddress         :: ServerAddress,
    psqlAddress          :: ServerAddress
    psqlUsername         :: String,
    psqlPassword         :: String,
    psqlDatabase         :: String,
    psqlConcurrency      :: Int,
    couchConcurrency     :: Int
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
  toSql idx = conventionalize "idx" name True
    where
      name = (toSql $ indexTable idx) ++ "." ++ (indexName idx)
      conventionalize memo [] _ = reverse memo
      conventionalize memo (c:cs) upper =
        case okC of
          False -> conventionalize memo cs True
          True  -> conventionalize (newC:memo) False
        where
          okC  = Char.isAlphaNum c
          newC =
            case upper of
              False -> c
              True -> Char.toUpper c

  inReaderT = flip runReaderT

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

initBackupConfig :: () -> IO BackupConfig
initBackupConfig = do
    env <- getEnvironment
    let readEnv = readFromEnv env
    let couchAddr = readCouchAddress readEnv
    let sqlAddr = readPsqlAddress readEnv
    return BackupConfig <$>
      pure (readEnv "PSQL_TABLE_HISTORY" "history") <*>
      pure (readEnv "PSQL_TABLE_DELETED" "deleted") <*>
      pure (readEnv "PSQL_VIEW_CURRENT"  "current") <*>
      pure (readCouchAddress readEnv) <*>
      pure (readPsqlAddress readEnv) <*>
      pure (readEnv "PSQL_USERNAME" "postgres") <*>
      pure (readEnv "PSQL_PASSWORD" ""        ) <*>
      pure (readEnv "PSQL_DATABASE" "postgres") <*>
      pure (readEnv $ read ("PSQL_CONCURRENCY" "100")  :: Int) <*>
      pure (readEnv $ read ("COUCH_CONCURRENCY" "20")  :: Int)
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
      decompress = \_ -> True,
      cookieJar = None
    }
  }
  manager <- liftIO $ HTTP.newManager settings
  let couchCall = (flip HTTP.httpLbs) manager
  inReaderT couchCall $ do
    checkCouchConnection
  let getCouch = return couchCall
  let killCouch = \_ -> return ()
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
  where
    applyArgs = map $ \f -> f config withPsql
    round0 = [
      ensureDbTable
    ]
    round1 = [
      ensureHistoryTable,
      ensureAttachmentsTable
    ]
    round2 = [
      ensureCurrentView
    ]

ensureHistoryTable :: BackupConfig -> WithPsql -> IO ()
ensureHistoryTable config withPsql = do
    ensureTableOrView tbl def
    _ <- mapConcurrently [
      ensureIndex withPsql (IndexName tbl "docRevIdentifier") True  "dbId, docId, revId",
      ensureIndex withPsql (IndexName tbl "docRevOrdering")   False "dbId, docId, revOrd",
      ensureIndex withPsql (IndexName tbl "firstSeenAt")      False "firstSeenAt",
      ensureIndex withPsql (IndexName tbl "lastSeenAt")       False "lastSeenAt",
      ensureIndex withPsql (IndexName tbl "deletedAt")        False "deletedAt"
    ]
  where
    dbTblSql = toSql $ dbTableName tbl
    tbl = historyTableName tbl
    tblSql = toSql tbl
    def =
      "CREATE TABLE IF NOT EXISTS " ++ tblSql ++ " ( " ++
      "id BIGSERIAL PRIMARY KEY, " ++
      "dbId BIGINT NOT NULL REFERENCES " ++ dbTblSql ++ "(id) ON DELETE RESTRICT ON UPDATE CASCADE, " ++
      [sql|
      docId VARCHAR NOT NULL,
      revId VARCHAR NOT NULL,
      revOrd INT NOT NULL DEFAULT -2147483648,
      firstSeenAt TIMESTAMPZ NOT NULL DEFAULT NOW(),
      lastSeenAt TIMESTAMPZ NOT NULL DEFAULT NOW(),
      deletedAt TIMESTAMPZ NULL DEFAULT NULL,
      content JSONB NULL DEFAULT NULL
      )
      |]

ensureAttachmentsTable :: WithPsql -> BackupConfig -> WithPsql -> IO ()
ensureAttachmentsTable = undefined

ensureCurrentView :: WithPsql -> BackupConfig -> WithPsql -> IO ()
ensureCurrentView = undefined

ensureTableOrView :: WithPsql -> TableName -> SqlDefinition -> IO ()
ensureTableOrView withPsql tbl def = do
  exists <- checkTable withPsql tbl
  case exists of
    True -> return ()
    False -> createTableOrView withPsql tbl def

ensureIndex :: WithPsql -> IndexName -> Unique -> ColumnsSql -> IO ()
ensureIndex withPsql idx uniq cols = undefined
