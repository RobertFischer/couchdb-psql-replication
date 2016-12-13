{-# LANGUAGE OverloadedStrings   #-} {-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MonadFailDesugaring #-}
{-# LANGUAGE Rank2Types          #-}

module Psql (module Psql) where

import qualified Database.PostgreSQL.Simple as Db
import qualified Data.Char as Char
import qualified Data.ByteString.Lazy as B
import qualified Data.Aeson as Json
import Database.PostgreSQL.Simple.FromRow ( fromRow, field )
import Data.Pool
import Data.String ( fromString )
import Data.List (intercalate)
import Data.ByteString.Lazy (ByteString)
import Couch hiding (Client)
import Config ( ReplConfig(..), logInfo, psqlHost, psqlPort, psqlUser )
import Codec.Compression.GZip
import Data.Maybe (fromJust)
import Prelude hiding ( (++) )
import Data.Monoid ( (<>) )

-- This makes OverloadedStrings actually work with string concatenation
(++) :: String -> String -> String
(++) = (<>)

class ToSql x where
  toSql :: x -> String

data Schema = ViewSchema | DataSchema deriving (Show, Read, Eq, Ord)
newtype SchemaName = SchemaName String deriving (Show, Read, Eq, Ord)
instance ToSql SchemaName where
  toSql (SchemaName(name)) = name

data Table =
    BaseTable | BaseObjectTable | DbTable | DocTable |
    RevTable | RevContentTable | AttTable | AttContentTable |
    CurrentDbTable | CurrentDocTable | CurrentAttTable | CurrentRevTable
  deriving (Show, Read, Eq, Ord)
newtype TableName = TableName (SchemaName, String) deriving (Show, Read, Eq, Ord)
instance ToSql TableName where
  toSql (TableName(schema, name)) = (toSql schema) ++ "." ++ name

newtype ColumnName = ColumnName (TableName, String) deriving (Show, Read, Eq, Ord)
instance ToSql ColumnName where
  toSql (ColumnName(table, name)) = (toSql table) ++ "." ++ name

type IndexColumns = String
newtype IndexName = IndexName (TableName, IndexColumns) deriving (Show, Read, Eq, Ord)
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
    withConn :: forall a . (Db.Connection -> IO a) -> IO a,
    schemaName :: Schema -> SchemaName,
    tableName :: Table -> TableName
  }
clientTableName :: Client -> Table -> TableName
clientTableName client = tableName client

clientTableSql :: Client -> Table -> String
clientTableSql client tbl = toSql $ clientTableName client tbl

clientSchemaName :: Client -> Schema -> SchemaName
clientSchemaName client = schemaName client

clientSchemaSql :: Client -> Schema -> String
clientSchemaSql client schema = toSql $ clientSchemaName client schema

-- |Represents a record of a revision (without any content)
data RevRecord = RevRecord
  {
    revRecordDb :: DbName,
    docRev :: DocRev
  }

instance Db.FromRow RevRecord where
  fromRow = RevRecord <$>
    (DbName <$> field) <*>
    (DocRev <$> (DocId <$> field) <*> (RevId <$> field <*> field))

revRecordDoc :: RevRecord -> DocId
revRecordDoc = Couch.docId . docRev

revRecordRev :: RevRecord -> RevId
revRecordRev = Couch.docRevId . docRev

tableSchema :: Table -> Schema
tableSchema table
  | table `elem` [CurrentRevTable, CurrentDbTable, CurrentDocTable, CurrentAttTable] = ViewSchema
  | otherwise = DataSchema

makeClient :: ReplConfig -> IO Client
-- ^Generate a client based on the replication configuration.
makeClient config = do
    pool <- createPool makeConn killConn 1 120 concurrency
    let client = Client {
        withConn = withResource pool,
        schemaName = SchemaName . schemaFunc,
        tableName = tableFunc
      }
    ensureDdl client
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
                    | table == CurrentRevTable = tableName' "Revs"
                    | otherwise = error ("Don't know table: " ++ (show table))
      where
        tableName' tblStr = TableName ( schemaName', tblStr )
        schemaName' = SchemaName $ schemaFunc $ tableSchema table
    makeConn = Db.connect connInfo
    killConn = \c -> (Db.rollback c) >> (Db.close c)
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
  ensureView  client CurrentRevTable
  ensureView  client CurrentDocTable
  ensureView  client CurrentDbTable
  ensureView  client CurrentAttTable
  ensureIndex client BaseTable "lastSyncAt"
  ensureIndex client BaseObjectTable "deletedAt"
  ensureIndex client BaseObjectTable "couchId"
  ensureIndex client RevTable "ord"

ensureIndex :: Client -> Table -> IndexColumns -> IO ()
ensureIndex client tbl colStr = do
    Config.logInfo $ "Ensuring index " ++ idxName ++ " on " ++ tblSql
    withConn client $ \conn -> do
      _ <- Db.execute_ conn $ fromString
        (
          "CREATE INDEX IF NOT EXISTS " ++ idxName
          ++ " ON " ++ tblSql ++ " (" ++ colStr ++ ") "
        )
      return ()
  where
    tblSql = toSql tblName
    tblName = clientTableName client tbl
    idxName = toSql $ IndexName (tblName, colStr)

ensureSchema :: Client -> Schema -> IO ()
ensureSchema client schema = do
    Config.logInfo $ "Ensuring schema " ++ name
    withConn client $ \conn -> do
      _ <- Db.execute_ conn $ fromString $ "CREATE SCHEMA IF NOT EXISTS " ++ name
      return ()
  where
    name = clientSchemaSql client schema

ensureView :: Client -> Table -> IO ()
ensureView client table = do
    Config.logInfo $ "Ensuring view " ++ name ++ " as: " ++ query
    withConn client $ \conn -> do
      _ <- Db.execute_ conn $ fromString $ "CREATE OR REPLACE VIEW " ++ name ++ " AS " ++ query
      return ()
  where
    name = clientTableSql client table
    query = viewQuerySql client table

viewQuerySql :: Client -> Table -> String
viewQuerySql client table
    | tableSchema table == DataSchema = error ("No view query SQL for a data schema table: " ++ show table)
    | table == CurrentDbTable =
        "SELECT DISTINCT ON (db.couchId) db.couchId AS name, "
        ++ " db.firstSyncAt AS firstSyncAt, db.lastSyncAt AS lastSyncAt"
        ++ " FROM " ++ dbTable ++ " db "
        ++ " INNER JOIN " ++ currDocTable ++ " doc  ON (doc.dbSqlId = db.id) "
        ++ " ORDER BY db.couchId, db.lastSyncAt DESC"
    | table == CurrentDocTable =
      "SELECT DISTINCT ON (db.couchId, doc.couchId) "
      ++ " db.couchId AS dbName, db.id AS dbSqlId, doc.couchId AS couchId, doc.id AS sqlId, "
      ++ " doc.firstSyncAt AS firstSyncAt, doc.lastSyncAt AS lastSyncAt "
      ++ " FROM " ++ docTable ++ " doc INNER JOIN " ++ dbTable ++ " db ON (doc.dbId = db.id) "
      ++ " INNER JOIN " ++ currRevTable ++ " rev ON (rev.docSqlId = doc.id) "
      ++ " WHERE db.deletedAt IS NULL AND doc.deletedAt IS NULL "
      ++ " ORDER BY db.couchId, doc.couchId, doc.lastSyncAt DESC "
    | table == CurrentRevTable =
      "SELECT DISTINCT ON (db.couchId, doc.couchId, rev.ord) "
      ++ " db.couchId AS dbName, db.id AS dbSqlId, "
      ++ " doc.couchId AS docCouchId, doc.id AS docSqlId, rev.ord AS ord, rev.id AS sqlId, "
      ++ " rev.couchId AS couchId, "
      ++ " CONCAT(rev.ord, '-', rev.couchId) AS fullId, "
      ++ " rev.lastSyncAt AS lastSyncAt, rev.firstSyncAt AS firstSyncAt "
      ++ " FROM " ++ revTable ++ " rev "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ " db ON (doc.dbId = db.id) "
      ++ " WHERE db.deletedAt IS NULL AND doc.deletedAt IS NULL AND rev.deletedAt IS NULL "
      ++ " ORDER BY db.couchId, doc.couchId, rev.ord, rev.lastSyncAt DESC "
    | table == CurrentAttTable =
      "SELECT DISTINCT ON (db.couchId, doc.couchId, att.couchId) "
      ++ " db.couchId AS dbName, doc.couchId AS docCouchId, doc.sqlId AS docSqlId, "
      ++ " rev.ord AS revOrd, att.couchId AS name, "
      ++ " att.id AS sqlId, content.attContent AS content, content.gzipped AS contentGzipped, "
      ++ " COALESCE(curRev.sqlId, rev.id) AS revSqlId, COALESCE(curRev.couchId, rev.couchId) AS revCouchId "
      ++ " FROM " ++ attTable ++ " att "
      ++ " INNER JOIN " ++ revTable ++ " rev ON (att.revId = rev.id) "
      ++ " INNER JOIN " ++ currDocTable ++ " doc ON (rev.docId = doc.sqlId) "
      ++ " INNER JOIN " ++ dbTable ++ " db ON (doc.dbSqlId = db.id) "
      ++ " INNER JOIN " ++ attContentTable ++ " content ON (att.id = content.attId) "
      ++ " LEFT JOIN " ++ currRevTable ++ " curRev ON (db.id = curRev.dbSqlId AND doc.sqlId = curRev.docSqlId AND rev.ord = curRev.ord) "
      ++ " ORDER BY db.couchId, doc.couchId, att.couchId, att.lastSyncAt DESC, content.lastSyncAt DESC"
    | otherwise = error ("Don't know table: " ++ (show table))
  where
    tableSql' = clientTableSql client
    dbTable = tableSql' DbTable
    currDocTable = tableSql' CurrentDocTable
    currRevTable = tableSql' CurrentRevTable
    attTable = tableSql' AttTable
    attContentTable = tableSql' AttContentTable
    revTable = tableSql' RevTable
    docTable = tableSql' DocTable


ensureTable :: Client -> Table -> IO ()
ensureTable client table = do
    Config.logInfo $ "Ensuring table " ++ name ++ " => " ++ sql
    withConn client $ \conn -> do
      _ <- Db.execute_ conn $ fromString sql
      return ()
  where
    sql = "CREATE TABLE IF NOT EXISTS " ++ name ++ " ( " ++ columns ++ " ) " ++ inheritance
    name = clientTableSql client table
    columns = tableColumnSql client table
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
    | tableSchema table == ViewSchema = error ("No column table SQL for a view schema table: " ++ show table)
    | table == BaseTable = "id BIGSERIAL PRIMARY KEY, firstSyncAt " ++ nowTime ++ ", lastSyncAt" ++ nowTime
    | table == BaseObjectTable = multi [ "couchId VARCHAR NOT NULL", "deletedAt TIMESTAMPTZ NULL DEFAULT NULL"]
    | table == DbTable = multi [ uniqueId, "UNIQUE (couchId)" ]
    | table == DocTable = multi [ uniqueId, refsTable "dbId" DbTable, "UNIQUE (dbId, couchId)" ]
    | table == RevTable = multi [
        uniqueId,
        refsTable "docId" DocTable,
        "ord SMALLINT NOT NULL",
        "UNIQUE (docId, couchId)"
      ]
    | table == RevContentTable = multi [
        uniqueId,
        refsTable "revId" RevTable,
        "revContent JSONB NOT NULL UNIQUE"
      ]
    | table == AttTable = multi [
        uniqueId,
        refsTable "revId" RevTable,
        "contentType VARCHAR NOT NULL",
        "UNIQUE (revId, couchId)"
      ]
    | table == AttContentTable = multi [
        uniqueId,
        refsTable "attId" AttTable,
        "attContent BYTEA NOT NULL",
        "gzipped BOOLEAN NOT NULL DEFAULT TRUE"
      ]
    | otherwise = error ("Table is not known: " ++ (show table))
  where
    uniqueId = "UNIQUE(id)"
    multi = intercalate ", "
    tableName' tbl = clientTableSql client tbl
    refsTable colName tbl = colName ++ " BIGINT REFERENCES " ++ (tableName' tbl) ++ " (id) " ++ refChange
    refChange = " ON UPDATE CASCADE ON DELETE RESTRICT "
    nowTime = " TIMESTAMPTZ NOT NULL DEFAULT NOW() "

checkRevisionExists :: Client -> DbName -> DocRev -> IO Bool
-- ^Determine if a given revision exists within the given database.
checkRevisionExists client db doc = do
    Config.logInfo $ "Checking if revision exists: DB[" ++ dbName ++ "] rev[" ++ rev ++ "]"
    withConn client $ \conn -> do
      result <- (Db.query conn sql [dbName, rev])
      return $ case result of
                 [] -> False
                 ((Db.Only x):[]) -> fromJust x
                 _ -> error "Multiple results returned when checking if a revision existed"
  where
    dbName = dbNameStr db
    rev = revId $ docRevId doc
    revTable = clientTableSql client RevTable
    docTable = clientTableSql client DocTable
    dbTable = clientTableSql client DbTable
    sql = fromString $ "SELECT EXISTS( SELECT * FROM " ++ revTable ++ " r " ++
      "INNER JOIN " ++ docTable ++ " doc ON (r.docId = doc.id) " ++
      "INNER JOIN " ++ dbTable ++ " d ON (doc.dbId = d.id) " ++
      "WHERE d.couchId = ? AND r.couchId = ? )"

withTransaction :: Client -> (Client -> IO a) -> IO a
-- ^Wraps 'Db.withTransaction' to use a connection from the given 'Client'. The
-- client passed into the second argument will always execute within the transaction.
withTransaction baseClient callback =
    withConn baseClient $ \conn ->
      Db.withTransaction conn $
        callback $ baseClient { withConn = \f -> f conn }

compress :: ByteString -> ByteString
compress bs = compressWith params bs
  where
    lengthBytes = fromIntegral $ B.length bs -- WARNING: forces the bytestring into memory
    maxToOne a = max 1 a
    lengthKb = maxToOne ((lengthBytes+1023) `quot` 1024)
    oneKb :: Int
    oneKb = 1024
    oneMb = 1024 * oneKb
    bufferSize = fromIntegral $ min oneMb (lengthBytes+1)
    lengthFactor = maxToOne $ logBase2Z lengthKb
    compression = compressionLevel $ min 9 lengthFactor
    params = defaultCompressParams
      {
        compressLevel = compression,
        compressWindowBits = windowBits 15, -- Maximum window bits
        compressMemoryLevel = maxMemoryLevel,
        compressBufferSize = bufferSize -- Used only for the initial buffer; always 16k after that
      }
    logBase2Z a = loop 0 (fromIntegral a)
      where
        loop :: Int -> Integer -> Int
        loop ctr val | val > 0   = loop (ctr+1) (val `quot` 2)
                     | otherwise = ctr

callDb :: Db.ToRow a => Client -> String -> a -> IO ()
callDb client sql params = do
  Config.logInfo $ "Executing db call: " ++ sql
  withConn client $ \conn -> do
    _ <- Db.execute conn (fromString sql) params
    return ()

ensureDatabase :: Client -> DbName -> IO ()
ensureDatabase client (DbName db) = do
    Config.logInfo $ "Ensuring database:\t" ++ dbTbl
    callDb client sql [db]
  where
    dbTbl = clientTableSql client DbTable
    sql = "INSERT INTO " ++ dbTbl ++ " (couchId) VALUES (?) " ++
      onConflictNoteSync

ensureDocument :: Client -> DbName -> DocId -> IO ()
ensureDocument client (DbName db) (DocId doc) = do
    Config.logInfo $ "Ensuring document:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "]"
    callDb client sql [db, doc]
  where
    sql = "INSERT INTO " ++ docTable ++ " (dbId, couchId) " ++
      " VALUES ( (SELECT id FROM " ++ dbTable ++ " WHERE couchId = ?), ?) " ++
      " ON CONFLICT (dbId, couchId) DO UPDATE SET lastSyncAt = NOW()"
    docTable = clientTableSql client DocTable
    dbTable = clientTableSql client DbTable

ensureRevision :: Client -> DbName -> DocRev -> IO ()
ensureRevision client (DbName db) dRev = do
    Config.logInfo $ "Ensuring revision:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "]" ++ " Rev[" ++ rev ++ "]"
    callDb client sql (db, doc, rev, ord)
  where
    sql = "INSERT INTO " ++ revTable ++ " (docId, couchId, ord) " ++
      " VALUES ( " ++
        " (SELECT d.id FROM " ++ docTable ++ " doc INNER JOIN " ++ dbTable ++ " db " ++
        " ON (db.id = doc.dbId) WHERE db.couchId = ? AND d.couchId = ? ) ," ++
        " ?, ?" ++
        " ) " ++ onConflictNoteSync ++ ", ord = EXCLUDED.ord"
    doc = docIdStr $ docId dRev
    rev' = docRevId dRev
    rev = revId rev'
    ord = revOrd rev'
    revTable = clientTableSql client RevTable
    docTable = clientTableSql client DocTable
    dbTable = clientTableSql client DbTable

ensureRevisionContent :: Client -> DbName -> DocRev -> DocDetails -> IO ()
ensureRevisionContent client (DbName db) dRev docDetails = do
    Config.logInfo $ "Ensuring revision content:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "]" ++ " Rev[" ++ rev ++ "]"
    callDb client sql (db, doc, rev, jsonb)
  where
    doc = docIdStr $ docId dRev
    rev = revId $ docRevId dRev
    jsonb = Json.encode $ docContent docDetails
    sql = "INSERT INTO " ++ revContTable ++ " (revId, revContent) VALUES ( "
      ++ " ( SELECT rev.id FROM " ++ revTable ++ " rev "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ "db ON (doc.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.couchId = ? ) ,"
      ++ " ? "
      ++ " ) " ++ onConflictNoteSync
    tblSql = clientTableSql client
    revContTable = tblSql RevContentTable
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

noteDeletedRevision :: Client -> DbName -> DocRev -> IO ()
noteDeletedRevision client (DbName db) dRev = do
    Config.logInfo $ "Note deleted revision:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "]" ++ " Rev[" ++ rev ++ "]"
    callDb client sql [db, doc, rev]
  where
    doc = docIdStr $ docId dRev
    rev = revId $ docRevId dRev
    sql = "UPDATE " ++ revTable ++ " rev SET deletedAt = LEAST(rev.deletedAt, NOW()), lastSyncAt = NOW() "
      ++ " FROM " ++ docTable ++ " doc INNER JOIN " ++ dbTable ++ " db ON (doc.dbId = db.id)"
      ++ " WHERE rev.docId = doc.id AND "
      ++ " db.couchId = ? AND doc.couchId = ? AND rev.couchId = ? "
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

noteDeletedDocument :: Client -> DbName -> DocId -> IO ()
noteDeletedDocument client (DbName db) (DocId doc) = do
    Config.logInfo $ "Note deleted document:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "]"
    callDb client sql [db, doc]
  where
    sql = "UPDATE " ++ docTable ++ " doc SET deletedAt = LEAST(doc.deletedAt, NOW()), lastSyncAt = NOW() "
      ++ " FROM " ++ dbTable ++ " db "
      ++ " WHERE doc.dbId = db.id AND db.couchId = ? and doc.couchId = ? "
    tblSql = clientTableSql client
    docTable = tblSql DocTable
    dbTable = tblSql DbTable

ensureAttachment :: Client -> DbName -> DocId -> AttachmentStub -> IO ()
ensureAttachment client (DbName db) (DocId doc) att = do
    Config.logInfo $ "Ensure attachment:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "] Name[" ++ name ++ "]"
    callDb client sql (db, doc, rOrd, name, contentType)
  where
    rOrd = attRevPos att
    name = attName att
    contentType = attContentType att
    sql = "INSERT INTO " ++ attTable ++ " (revId, couchId, contentType) VALUES ( "
      ++ " ( SELECT rev.id FROM " ++ revTable ++ " rev "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ "db ON (doc.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.ord = ? "
      ++ " ORDER BY rev.deletedAt DESC NULLS LAST, rev.lastSyncAt DESC"
      ++ " LIMIT 1 ) ,"
      ++ " ?, ? "
      ++ " ) " ++ onConflictNoteSync ++ ", contentType = EXCLUDED.contentType"
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable
    attTable = tblSql AttTable

ensureAttachmentContent :: Client -> DbName -> DocId -> AttachmentStub -> ByteString -> IO ()
ensureAttachmentContent client (DbName db) (DocId doc) att content = do
    Config.logInfo $ "Ensure attachment content:\tDB[" ++ db ++ "] Doc[" ++ doc ++ "] Name[" ++ name ++ "]"
    callDb client sql (db, doc, rOrd, name, binContent)
  where
    binContent = Db.Binary $ Psql.compress content
    name = attName att
    rOrd = attRevPos att
    tblSql = clientTableSql client
    revTable = tblSql RevTable
    docTable = tblSql DocTable
    dbTable = tblSql DbTable
    attTable = tblSql AttTable
    attContentTable = tblSql AttContentTable
    sql = "INSERT INTO " ++ attContentTable ++ " cnt (gzipped, attId, attContent) VALUES (True, "
      ++ " ( SELECT attId FROM " ++ attTable ++ " att "
      ++ " INNER JOIN " ++ revTable ++ " rev ON (att.revId = rev.id) "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (rev.docId = doc.id) "
      ++ " INNER JOIN " ++ dbTable ++ " db ON (rev.dbId = db.id) "
      ++ " WHERE db.couchId = ? AND doc.couchId = ? AND rev.ord = ? AND att.couchId = ? "
      ++ " ORDER BY rev.deletedAt DESC NULLS LAST, att.lastSyncAt DESC, rev.lastSyncAt DESC "
      ++ " LIMIT 1 ), "
      ++ " ? "
      ++ " ) "

fetchLiveRevisions :: Client -> IO [RevRecord]
fetchLiveRevisions client = do
    Config.logInfo $ "Fetch live revisions"
    withConn client $ \conn -> Db.query_ conn (fromString sql)
  where
    sql = "SELECT DISTINCT ON(db.id, doc.id) db.couchId, doc.couchId, rev.ord, rev.couchId "
      ++ " FROM " ++ dbTable ++ " db "
      ++ " INNER JOIN " ++ docTable ++ " doc ON (doc.dbId = db.id) "
      ++ " INNER JOIN " ++ revTable ++ " rev ON (rev.docId = doc.id) "
      ++ " WHERE doc.deletedAt IS NULL AND rev.deletedAt IS NULL "
      ++ " ORDER BY db.id, doc.id, doc.firstSyncAt, rev.ord DESC "
    dbTable = clientTableSql client DbTable
    docTable = clientTableSql client DocTable
    revTable = clientTableSql client RevTable

onConflictNoteSync :: String
onConflictNoteSync = "ON CONFLICT (couchId) DO UPDATE SET lastSyncAt = NOW()"
