{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}

module Psql (module Psql) where

import qualified Database.PostgreSQL.Simple as Db
import qualified Data.Char
import qualified Data.ByteString.Lazy as B
import qualified Data.Aeson as Json
import Data.Pool
import Data.List (intercalate)
import Data.ByteString.Lazy (ByteString)
import Couch ( DbName(..), DocRev(..), DocId(..), RevId(..), DocDetails, AttachmentStub )
import Control.Exception
import Config ( ReplConfig )
import Codec.Compression.GZip

class ToSql x where
  toSql :: x -> String

data Schema = ViewSchema | DataSchema deriving (Show, Read, Eq, Ord, Generic)
newtype SchemaName = SchemaName String deriving (Show, Read, Eq, Ord, Generic)
instance ToSql SchemaName where
  toSql (SchemaName(name)) = name

data Table =
    BaseTable | BaseObjectTable | DbTable | DocTable |
    RevTable | RevContentTable | AttTable | AttContentTable |
    CurrentDbTable | CurrentDocTable | CurrentAttTable
  deriving (Show, Read, Eq, Ord, Generic)
newtype TableName = TableName (SchemaName, String) deriving (Show, Read, Eq, Ord, Generic)
instance ToSql TableName where
  toSql (TableName(schema, name)) = (toSql schema) ++ "." ++ name

newtype ColumnName = Column (TableName, String) deriving (Show, Read, Eq, Ord, Generic)
instance ToSql ColumnName where
  toSql (ColumnName(table, name)) = (toSql table) ++ "." ++ name

type IndexColumns = String
newtype IndexName = Index (TableName, IndexColumns) deriving (Show, Read, Eq, Ord, Generic)
instance ToSql IndexName where
  toSql (IndexName(table,cols)) = (++) "idx" $ conventionalize name
    where
      name = (toSql table) ++ "Table." ++ cols
      cap [] = []
      cap (x:xs) = (Char.toUpper x) : xs
      conventionalize [] = []
      conventionalize (x:xs) = cap $ conv' x xs
      checkX = Char.isAlphaNum
      conv' x []     | checkX x  = [x]
                     | otherwise = []
      conv' x (y:ys) | checkX x  = x:    (conv' y ys)
                     | otherwise = cap $ (conv' y ys)

-- |Represents a client to the PSQL synchronization
data Client = Client
  {
    getConn :: IO Db.Connection,
    schemaName :: Schema -> SchemaName,
    tableName :: Table -> TableName
  }
clientTableName :: Client -> Table -> TableName
clientTableName client = tableName client

clientTableSql :: Client -> Table -> String
clientTableSql = toSql . clientTableName

clientSchemaName :: Client -> Schema -> SchemaName
clientSchemaName client = schemaName client

clientSchemaSql :: Client -> Schema -> String
clientSchemaSql = toSql . clientSchemaName

-- |Represents a record of a revision (without any content)
data RevRecord = RevRecord
  {
    revRecordDb :: DbName,
    docRev :: DocRev
  }

instance FromRow RevRecord where
  fromRow = RevRecord
    {
      revRecordDb = (DbName <$> field),
      docRev = DocRev (DocId <$> field) (RevId <$> field <*> field)
    }

revRecordDoc :: RevRecord -> DocId
revRecordDoc = Couch.docId . docRev

revRecordRev :: RevRecord -> RevId
revRecordRev = Couch.docRevId . docRev

tableSchema :: Table -> Schema
tableSchema table
  | table `elem` [CurrentDbTable, CurrentDocTable, CurrentAttTable] = ViewSchema
  | otherwise = DataSchema

makeClient :: ReplConfig -> IO Client
-- ^Generate a client based on the replication configuration.
makeClient config = do
    pool <- createPool makeConn killConn 1 120 concurrency
    let client = Client
      {
        getConn = pool,
        schemaName = schemaFunc,
        tableName = tableFunc
      }
    onException (ensureDdl client) schemaError
    return client
  where
    viewSchemaName = Config.psqlSchemaName config
    dataSchemaName = viewSchemaName ++ "Data"
    schemaFunc ViewSchema = viewSchemaName
    schemaFunc DataSchema = dataSchemaName
    tableFunc table | table == BaseTable = tableName' "GlobalParent"
                    | table == BaseObjectTable = tableName' "ObjectParent"
                    | table == DbTable = tableName' "Dbs"
                    | table == DocTable = tableName' "Docs"
                    | table == RevTable = tableName' "Revs"
                    | table == RevContentTable = tableName' "RevContents"
                    | table == AttTable = tableName' "Atts"
                    | table == AttContentTable = tableName' "AttContents"
                    | table == CurrentDbTable = tableName' "Dbs"
                    | table == CurrentDocTable = tableName' "Docs"
                    | table == CurrentAttTable = tableName' "Atts"
      where
        tableName' = TableName ( schemaName', tblStr )
        schemaName' = SchemaName $ schemaFunc $ tableSchema table
    makeConn = Db.connect connInfo
    makeErr e = do
      Config.logError $ "Error creating a connection: " ++ (displayException e)
      throw e
    schemaErr e = Config.logWarn $ "Error creating a schema: " ++ (displayException e)
    killConn = finally Db.rollback $ catch Db.close killErr
    killErr e = Config.logWarn $ "Error closing connection: " ++ (displayException e)
    concurrency = Config.psqlConcurrency config
    connInfo = Db.defaultConnectInfo
      {
        Db.connectHost = Config.psqlHost config,
        Db.connectPort = Config.psqlPort config,
        Db.connectUser = Config.psqlUser config,
        Db.connectPassword = Config.psqlPass config,
        Db.connectDatabase = Config.psqlDb config
      }

ensureDdl :: Client -> IO ()
ensureDdl baseClient = withTransaction baseClient $ \client -> do
  ensureSchema client ViewSchema
  ensureSchema client DataSchema
  ensureTable client BaseTable
  ensureTable client BaseObjectTable
  ensureTable client DbTable
  ensureTable client DocTable
  ensureTable client RevTable
  ensureTable client RevContentTable
  ensureTable client AttTable
  ensureTable client AttContentTable
  ensureView  client CurrentDbTable
  ensureView  client CurrentDocTable
  ensureView  client CurrentAttTable

ensureSchema :: Client -> Schema -> IO ()
ensureSchema client schema = do
    conn  <- getConnection client
    _ <- execute_ $ "CREATE SCHEMA IF NOT EXISTS " ++ name
  where
    name = clientSchemaSql client schema

ensureView :: Client -> Table -> IO ()
ensureView client table = do
    conn <- getConnection client
    _ <- execute_ $ "CREATE OR REPLACE VIEW " ++ name ++ " AS " ++ query
  where
    name = clientTableSql client table
    query = viewQuerySql client table

ensureTable :: Client -> Table -> IO ()
ensureTable client table = do
    conn <- getConnection client
    _ <- execute_ $
      "CREATE TABLE IF NOT EXISTS " ++ name ++
      " ( " ++ columns ++ " ) " ++ inheritance
  where
    name = clientTableSql client table
    columns = tableColumnSql table
    parentTables = (clientTableSql client) <$> (tableInheritance table)
    inheritance = wrapParents $ intercalate ", " parentTables
    wrapParents "" = ""
    wrapParents tableSql = "INHERITS ( " ++ tableSql ++ " )"

tableInheritance :: Table -> [Table]
tableInheritance table | tableSchema table == ViewSchema = []
tableInheritance BaseTable = []
tableInheritance BaseObjectTable = []
tableInheritance RevContentTable = [BaseTable]
tableInheritance AttContentTable = [BaseTable]
tableInheritance _ = [BaseTable, BaseObjectTable]

tableColumnSql :: Client -> Table -> String
tableColumnSql client table
    | tableSchema table == ViewSchema = error "No column table SQL for the view schema"
    | table == BaseTable = "id BIGSERIAL PRIMARY KEY, firstSyncAt " ++ nowTime ++ ", lastSyncAt" ++ nowTime
    | table == BaseObjectTable = "couchId VARCHAR NOT NULL"
    | table == DbTable = "UNIQUE (couchId)"
    | table == DocTable = multi [ refsTable "dbId" DbTable, "deletedAt TIMESTAMPZ NULL DEFAULT NULL", "UNIQUE (dbId, couchId)" ]
    | table == RevTable = multi [
        refsTable "docId" DocTable,
        "ord SMALLINT NOT NULL",
        "deletedAt TIMESTAMPZ NULL DEFAULT NULL",
        "UNIQUE (docId, couchId)"
      ]
    | table == RevContentTable = multi [
        refsTable "revId" RevTable,
        "revContent JSONB NOT NULL UNIQUE"
      ]
    | table == AttTable = multi [
        refsTable "revId" RevTable,
        "contentType VARCHAR NOT NULL",
        "UNIQUE (revId, couchId)"
      ]
    | table == AttContentTable = multi [
        refsTable "attId" AttTable,
        "attContent BYTEA NOT NULL UNIQUE",
        "gzipped BOOLEAN NOT NULL DEFAULT TRUE"
      ]
  where
    multi = intercalate ", "
    tableName' = clientTableSql client
    refsTable colName tbl = colName ++ " BIGINT REFERENCES " ++ (tableName' tbl)
    refChange = "ON UPDATE CASCADE ON DELETE RESTRICT"
    nowTime = "TIMESTAMPZ NOT NULL DEFAULT NOW()"

checkRevisionExists :: Client -> DbName -> DocRev -> IO Bool
-- ^Determine if a given revision exists within the given database.
checkRevisionExists client db doc = do
    conn <- getConn client)
    fromJust <$> query conn sql [dbName, rev]
  where
    dbName = dbNameStr db
    rev = revId $ docRevId doc
    revTable = clientTableSql client RevTable
    dbTable = clientTableSql client DbTable
    sql = "SELECT EXISTS( SELECT * FROM " ++ revTable ++ " r " ++
      "INNER JOIN " ++ dbTable ++ " d ON (r.dbId = d.id) " ++
      "WHERE d.couchId = ? AND r.couchId = ? )"

withTransaction :: Client -> (Client -> IO a) -> IO a
-- ^Wraps 'Db.withTransaction' to use a connection from the given 'Client'. The
-- client passed into the second argument will always execute within the transaction.
withTransaction baseClient callback = do
  conn <- getConn baseClient
  withTransaction conn $
    callback $ baseClient { Db.getConn = return conn }

compress :: ByteString -> ByteString
compress bs = compressWith params bs
  where
    lengthBytes = B.length bs -- WARNING: forces the bytestring into memory
    ceilToOne a = max 1 $ (fromIntegral . ceiling) a
    lengthKb = ceilToOne $ lengthBytes / 1024.0
    oneKb = 1024
    oneMb = 1024 * oneKb
    bufferSize = min oneMb (lengthBytes+1)
    lengthFactor = ceilToOne $ logBase 2 lengthKb
    compression = compressionLevel $ min 9 lengthFactor
    params = defaultCompressParams
      {
        compressLevel = compression,
        compressWindowBits = windowBits 15, -- Maximum window bits
        compressMemoryLevel = maxMemoryLevel,
        compressBufferSize = bufferSize -- Used only for the initial buffer; always 16k after that
      }

callDb :: FromJson a => Client -> Query -> a -> IO ()
callDb client query params = do
  Config.info $ "Executing db call: " ++ (show query)
  conn <- getConn client
  _ <- execute conn sql params
  return ()

ensureDatabase :: Client -> DbName -> IO ()
ensureDatabase client DbName(db) = callDb client sql (Only db)
  where
    dbTbl = clientTableSql client DbTable
    sql = "INSERT INTO " ++ dbTbl ++ " (couchId) VALUES (?) " ++
      onConflictNoteSync

ensureDocument :: Client -> DbName -> DocId -> IO ()
ensureDocument client DbName(db) DocId(doc) = callDb client sql [db, doc]
  where
    sql = "INSERT INTO " ++ docTable ++ " (dbId, couchId) " ++
      "VALUES (SELECT id FROM " ++ dbTable ++ " WHERE couchId = ?, ?) " ++
      onConflictNoteSync
    docTable = clientTableSql client DocTable
    dbTable = clientTableSql client DbTable

ensureRevision :: Client -> DbName -> DocRev -> IO ()
ensureRevision client DbName(db) docRev = callDb client sql [db, doc, rev, ord]
  where
    sql = "INSERT INTO " ++ revTable ++ " (docId, couchId, ord) " ++
      " VALUES ( " ++
        " SELECT d.id FROM " ++ docTable ++ " doc INNER JOIN " ++ dbTable ++ " db " ++
        " ON (db.id = doc.dbId) WHERE db.couchId = ? AND d.couchId = ? ," ++
        " ?, ?" ++
        " ) " ++ onConflictNoteSync ++ ", ord = EXCLUDED.ord"
    doc = docIdStr $ docId docRev
    rev' = docRevId docRev
    rev = revId rev'
    ord = revOrd rev'
    revTable = clientTableSql client RevTable
    docTable = clientTableSql client DocTable
    dbTable = clientTableSql client DbTable

ensureRevisionContent :: Client -> DbName -> DocRev -> DocDetails -> IO ()
ensureRevisionContent client DbName(db) docRev docDetails =
    callDb client sql [db, doc, rev, jsonb]
  where
    doc = docIdStr $ docId docRev
    rev = revId $ docRevId docRev
    jsonb = Json.encode $ docContent docDetails
    sql = "INSERT INTO " ++ revContTable ++ " (revId, revContent) VALUES ( "
      ++ " SELECT rev.id FROM " ++ revTable ++ " rev "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ "db ON (doc.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.couchId = ? ,"
      ++ " ? "
      ++ " ) " ++ onConflictNoteSync
    tblSql = clientTableSql client
    revContTable = tblSql RevContentTable
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

noteDeletedRevision :: Client -> DbName -> DocRev -> IO ()
noteDeletedRevision client DbName(db) docRev = callDb client sql [db, doc, rev]
  where
    doc = docIdStr $ docId docRev
    rev = revId $ docRevId docRev
    sql = "UPDATE " ++ revTable ++ " rev SET deletedAt = COALESCE(rev.deletedAt, NOW()), lastSyncAt = NOW() "
      ++ " FROM " ++ docTable ++ " doc INNER JOIN " ++ dbTable ++ " db ON (doc.dbId = db.id)"
      ++ " WHERE rev.docId = doc.id AND "
      ++ " db.couchId = ? AND doc.couchId = ? AND rev.couchId = ? "
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

noteDeletedDocument :: Client -> DbName -> DocId -> IO ()
noteDeletedDocument client DbName(db) DocId(doc) = callDb client sql [db, doc]
  where
    sql = "UPDATE " ++ docTable ++ " doc SET deletedAt = COALESCE(doc.deletedAt, NOW()), lastSyncAt = NOW() "
      ++ " FROM " ++ dbTable ++ " db "
      ++ " WHERE doc.dbId = db.id AND db.couchId = ? and doc.couchId = ? "
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

ensureAttachment :: Client -> DbName -> DocId -> AttachmentStub -> IO ()
ensureAttachment client DbName(db) DocId(doc) att = callDb client sql [db, doc, revOrd, name, contentType]
  where
    revOrd = attRevPos att
    name = attName att
    contentType = attContentType att
    sql = "INSERT INTO " ++ attTable ++ " (revId, couchId, contentType) VALUES ( "
      ++ " SELECT rev.id FROM " ++ revTable ++ " rev "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ "db ON (doc.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.ord = ? "
      ++ " ORDER BY rev.deletedAt DESC NULLS LAST, rev.lastSyncAt DESC"
      ++ " LIMIT 1 "
      ++ " , ?, ? "
      ++ " ) " ++ onConflictNoteSync ++ ", contentType = EXCLUDED.contentType"
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable
    attTable = tblSql AttTable

ensureAttachmentContent :: Client -> DbName -> DocId -> AttachmentStub -> ByteString -> IO ()
ensureAttachmentContent client DbName(db) DocId(doc) att content = callDb [db, doc, revOrd, name, binContent]
  where
    binContent = Binary $ compress content
    name = attName att
    revOrd = attRevPos att
    sql = "INSERT INTO " ++ attContentTable ++ " cnt (gzipped, attId, attContent) VALUES (True, "
      ++ " SELECT attId FROM " ++ attTable ++ " att "
      ++ " INNER JOIN " ++ revTable ++ " rev ON (att.revId = rev.id) "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ " db ON (rev.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.ord = ? AND att.couchId = ? "
      ++ " ORDER BY rev.deletedAt DESC NULLS LAST, att.lastSyncAt DESC, rev.lastSyncAt DESC "
      ++ " LIMIT 1 "
      ++ ", ?"
      ++ " ) " ++ onConflictNoteSync ++ ", gzipped = EXCLUDED.gzipped, attContent = EXCLUDED.attContent "

fetchLiveRevisions :: Client -> IO [RevRecord]
fetchLiveRevisions client = do
    conn <- getConn client
    query_ conn sql
  where
    sql = "SELECT DISTINCT ON(db.id, doc.id) db.couchId, doc.couchId, rev.ord, rev.couchId "
      ++ " FROM " ++ dbTable ++ " db "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (doc.dbId = db.id) "
      ++ " INNER JOIN " ++ revTable ++ " rev ON (rev.docId = doc.id) "
      ++ " WHERE doc.deletedAt IS NULL AND rev.deletedAt IS NULL "
      ++ " ORDER BY db.couchId, doc.firstSyncAt, rev.ord DESC "
    dbTable = clientTableSql client DbTable
    docTable = clientTableSql client DocTable
    revTable = clientTableSql client RevTable

onConflictNoteSync = "ON CONFLICT DO UPDATE SET lastSyncAt = NOW()"
