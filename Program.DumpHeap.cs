using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.SqlServer.Server;
using output2db.esr;

namespace output2db
{
    partial class Program
    {
        private class DumpHeapItem
        {
            public readonly long Address;
            public readonly long MT;
            public readonly long Size;

            public const string UDT_NAME = "dbo.DumpHeapItem";

            public static readonly SqlMetaData[] SqlMetadata = new[]
            {
                new SqlMetaData("Address", SqlDbType.BigInt),
                new SqlMetaData("MT", SqlDbType.BigInt),
                new SqlMetaData("Size", SqlDbType.BigInt)
            };

            public DumpHeapItem(string line)
            {
                var parts = line.Split(s_spaceDelim, StringSplitOptions.RemoveEmptyEntries);
                Address = long.Parse(parts[0], NumberStyles.HexNumber);
                MT = long.Parse(parts[1], NumberStyles.HexNumber);
                Size = long.Parse(parts[2]);
            }

            public static void PopulateSqlRecord(DumpHeapItem o, SqlDataRecord rec)
            {
                rec.SetInt64(0, o.Address);
                rec.SetInt64(1, o.MT);
                rec.SetInt64(2, o.Size);
            }
        }

        private static void PersistDumpHeap(SqlConnection conn, JournalEntry j, string table, int batchSize, long fileLength, Action<long> fnWriteProgress)
        {
            const string SQL_FMT = @"
BEGIN TRAN
INSERT INTO [{0}] (Address, MT, Size)
SELECT Address, MT, Size
FROM @batch

UPDATE Journal SET 
    LineOffset = @LineOffset,
    ByteOffset = @ByteOffset,
    PercentDone = @PercentDone
WHERE Id = @JournalId
COMMIT";

            fnWriteProgress(0);
            try
            {
                var sr = new ExtendedStreamReader();
                var skips = j.ByteOffset == 0 ? null : new[] { new Skip(0, 0, j.LineOffset - 1, j.ByteOffset) };
                var sql = string.Format(SQL_FMT, table);
                var buffer = new List<DumpHeapItem>(batchSize);
                LineItem lastLineItem = null;
                foreach (var lineItem in sr.GetLineSource(new FileStream(j.FilePath, FileMode.Open), skips: skips)
                    .SkipWhile(lineItem => lineItem.Line != "         Address               MT     Size")
                    .Skip(1)
                    .TakeWhile(lineItem => lineItem.Line.Length > 0)
                    .ToEnumerable())
                {
                    lastLineItem = lineItem;
                    var o = new DumpHeapItem(lineItem.Line);
                    buffer.Add(o);
                    if (buffer.Count == batchSize)
                    {
                        PersistBatch(conn, sql, buffer, j.Id, lineItem.LineOffset + lineItem.LineCount, lineItem.ByteOffset + lineItem.ByteCount, fileLength, false, fnWriteProgress);
                        buffer.Clear();
                    }
                }
                if (buffer.Count > 0)
                {
                    PersistBatch(conn, sql, buffer, j.Id, lastLineItem.LineOffset + lastLineItem.LineCount, lastLineItem.ByteOffset + lastLineItem.ByteCount, fileLength, true, fnWriteProgress);
                }
                CloseJournal(conn, j.Id);
                fnWriteProgress(-1);
            }
            finally
            {
                fnWriteProgress(-2);
            }
        }

        private static void CloseJournal(SqlConnection conn, int journalId)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "UPDATE Journal SET PercentDone = 100 WHERE Id = @JournalId";
                cmd.Parameters.Add(new SqlParameter("JournalId", journalId));
                cmd.ExecuteNonQuery();
            }
        }

        private static void PersistBatch(SqlConnection conn, string sql, IEnumerable<DumpHeapItem> buffer, int journalId, long lineOffset, long byteOffset, long fileLength, bool done,
            Action<long> fnWriteProgress)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.Parameters.Add(CreateSqlParameter(buffer, "batch", DumpHeapItem.UDT_NAME, DumpHeapItem.SqlMetadata, DumpHeapItem.PopulateSqlRecord));
                cmd.Parameters.Add(new SqlParameter("LineOffset", lineOffset + 1));
                cmd.Parameters.Add(new SqlParameter("ByteOffset", byteOffset));
                cmd.Parameters.Add(new SqlParameter("PercentDone", (int)((byteOffset * 100.0 / fileLength) * 100) / 100.0));
                cmd.Parameters.Add(new SqlParameter("JournalId", journalId));
                cmd.ExecuteNonQuery();
            }
            fnWriteProgress(byteOffset);
        }
    }
}
