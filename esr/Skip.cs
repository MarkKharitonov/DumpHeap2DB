using System.Data;

namespace output2db.esr
{
    public class Skip : IFileChunk
    {
        public long LineOffset { get; private set; }

        public long ByteOffset { get; private set; }

        public readonly long NextLineOffset;

        public readonly long NextByteOffset;

        public int LineCount
        {
            get { return (int)(NextLineOffset - LineOffset); }
        }

        public int ByteCount
        {
            get { return (int)(NextByteOffset - ByteOffset); }
        }

        public Skip(IDataRecord reader)
            : this(
                reader.GetInt64(reader.GetOrdinal("LineOffset")),
                reader.GetInt64(reader.GetOrdinal("ByteOffset")),
                reader.GetInt64(reader.GetOrdinal("NextLineOffset")),
                reader.GetInt64(reader.GetOrdinal("NextByteOffset"))
                )
        {
        }

        public Skip(long lineOffset, long byteOffset, long nextLineOffset, long nextByteOffset)
        {
            LineOffset = lineOffset;
            ByteOffset = byteOffset;
            NextLineOffset = nextLineOffset;
            NextByteOffset = nextByteOffset;
        }
    }
}
