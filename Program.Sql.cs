using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Microsoft.SqlServer.Server;

namespace output2db
{
    partial class Program
    {
        private static SqlConnection SetupDB(string connectionString, string filePath, string table, bool force, out JournalEntry j, bool verbose)
        {
            var conn = new SqlConnection(connectionString);
            try
            {
                try
                {
                    conn.Open();
                }
                catch (SqlException)
                {
                    if (verbose)
                    {
                        Console.WriteLine("Creating the database");
                    }
                    // Create the database
                    CreateDB(conn);
                    while (conn.State != ConnectionState.Open)
                    {
                        try
                        {
                            conn.Open();
                        }
                        catch (SqlException)
                        {
                            Thread.Sleep(100);
                        }
                    }
                }

                j = SetupTable(conn, filePath, table, force);
            }
            catch (Exception)
            {
                try { conn.Dispose(); } catch { }
                throw;
            }
            return conn;
        }

        private static JournalEntry SetupTable(SqlConnection conn, string filePath, string table, bool force)
        {
            // Sql injection - definitely.
            // Here I want to avoid the sp_executeSql call and see the SQL directly
            const string CREATE_SCHEMA_SQL_FMT = @"
DECLARE @Force BIT = {1}
DECLARE @CreateTable BIT = 1
DECLARE @FilePath NVARCHAR(256) = '{2}'
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}')
    IF @Force = 1
    BEGIN
        PRINT 'Dropping the table [{0}]'
        DROP TABLE [{0}]
    END
    ELSE
        SET @CreateTable = 0

IF @CreateTable = 1
BEGIN
    PRINT 'Creating the table [{0}]'
    CREATE TABLE [{0}]
    (
        Address BIGINT NOT NULL CONSTRAINT [PK_{0}_Address] PRIMARY KEY,
        MT BIGINT NOT NULL,
        Size BIGINT NOT NULL
    )

    UPDATE Journal SET
        LineOffset = 1,
        ByteOffset = 0,
        PercentDone = 0
    WHERE FilePath = @FilePath
    
    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO Journal (FilePath) VALUES (@FilePath)
        PRINT 'Added the journal entry'
    END
    ELSE
        PRINT 'Cleared the journal entry'
END

IF EXISTS (SELECT 1 FROM sys.types WHERE is_table_type = 1 AND name = 'DumpHeapItem')
    DROP TYPE DumpHeapItem

CREATE TYPE DumpHeapItem AS TABLE
(
    Address BIGINT NOT NULL PRIMARY KEY,
    MT BIGINT NOT NULL,
    Size BIGINT NOT NULL
)

SELECT Id, FilePath, LineOffset, ByteOffset, PercentDone 
FROM Journal 
WHERE FilePath = @FilePath";

            conn.InfoMessage += OnInfoMessage;
            try
            {
                using (var command = new SqlCommand(string.Format(CREATE_SCHEMA_SQL_FMT, table, force ? "1" : "0", filePath), conn))
                using (var reader = command.ExecuteReader())
                {
                    reader.Read();
                    return new JournalEntry(reader);
                }
            }
            finally
            {
                conn.InfoMessage -= OnInfoMessage;
            }
        }

        private static void OnInfoMessage(object sender, SqlInfoMessageEventArgs args)
        {
            foreach (SqlError err in args.Errors)
            {
                if (err.Number == 0)
                {
                    Console.WriteLine(err.Message);
                }
            }
        }

        private static void CreateDB(SqlConnection conn)
        {
            // Sql injection - definitely.
            // Here I want to avoid the sp_executeSql call and see the SQL directly
            const string CREATE_DB_SQL_FMT = @"
DECLARE @DBName VARCHAR(128) = '{0}'
IF NOT EXISTS (SELECT 1 FROM sysdatabases WHERE name = @DBName)
BEGIN
    DECLARE @DataPath NVARCHAR(256) = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(256))
    IF @DataPath IS NULL 
        RAISERROR('Could not figure out the default data path', 20, -1) WITH LOG

    IF @DataPath LIKE '%\'
        SET @DataPath = @DataPath + @DBName
    ELSE
        SET @DataPath = @DataPath + '\' + @DBName

    DECLARE @Sql NVARCHAR(1000) = '
        CREATE DATABASE [' + @DBName + '] CONTAINMENT = NONE
        ON PRIMARY (NAME = N''' + @DBName + ''', FILENAME = N''' + @DataPath + '.mdf'', SIZE = 4096KB, FILEGROWTH = 1024KB)
        LOG ON (NAME = N''' + @DBName + '_log'', FILENAME = N''' + @DataPath + '_log.ldf'', SIZE = 4096KB, FILEGROWTH = 10%)
    '
    EXEC(@Sql)
    DECLARE @State INT
    WHILE @State IS NULL OR @State <> 0
    BEGIN
        SELECT @State = State FROM sys.databases WHERE name = @DBName
        WAITFOR DELAY '00:00:01'
    END
END

EXEC('
USE [{0}]
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''Journal'')
    CREATE TABLE [Journal]
    (
        [Id] INT NOT NULL IDENTITY CONSTRAINT PK_Journal_Id PRIMARY KEY, 
        [FilePath] NVARCHAR(256) NOT NULL CONSTRAINT UC_Journal_FilePath UNIQUE, 
        [LineOffset] BIGINT NOT NULL CONSTRAINT DF_Journal_LineOffset DEFAULT 1, 
        [ByteOffset] BIGINT NOT NULL CONSTRAINT DF_Journal_ByteOffset DEFAULT 0, 
        [PercentDone] FLOAT NOT NULL CONSTRAINT DF_Journal_PercentDone DEFAULT 0
    )
')
";
            var csb = new DbConnectionStringBuilder();
            csb.ConnectionString = conn.ConnectionString;
            var database = (string)csb["database"];
            csb["database"] = "master";

            using (var masterConn = new SqlConnection(csb.ToString()))
            {
                masterConn.Open();
                using (var command = new SqlCommand(string.Format(CREATE_DB_SQL_FMT, database), masterConn))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        public static SqlParameter CreateSqlParameter<T>(this IEnumerable<T> value, string name, string udtName, SqlMetaData[] metadata, Action<T, SqlDataRecord> fnPopulateRecord)
        {
            return new SqlParameter(name, SqlDbType.Structured)
            {
                TypeName = udtName,
                SqlValue = YieldSqlDataRecords(value, metadata, fnPopulateRecord),
            };
        }

        private static IEnumerable<SqlDataRecord> YieldSqlDataRecords<T>(IEnumerable<T> value, SqlMetaData[] metadata, Action<T, SqlDataRecord> fnPopulateRecord)
        {
            return value.Select(x =>
            {
                var rec = new SqlDataRecord(metadata);
                fnPopulateRecord(x, rec);
                return rec;
            });
        }
    }
}
