using System.Data;

namespace output2db
{
    public class JournalEntry
    {
        public readonly int Id;
        public readonly string FilePath;
        public readonly double PercentDone;
        public readonly long LineOffset;
        public readonly long ByteOffset;

        public JournalEntry(IDataRecord reader)
        {
            Id = reader.GetInt32(reader.GetOrdinal("Id"));
            PercentDone = reader.GetDouble(reader.GetOrdinal("PercentDone"));
            FilePath = reader.GetString(reader.GetOrdinal("FilePath"));
            LineOffset = reader.GetInt64(reader.GetOrdinal("LineOffset"));
            ByteOffset = reader.GetInt64(reader.GetOrdinal("ByteOffset"));
        }
    }
}
