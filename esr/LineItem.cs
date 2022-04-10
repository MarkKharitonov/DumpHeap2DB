namespace output2db.esr
{
    public class LineItem : IFileChunk
    {
        public string Line { get; private set; }

        public long ByteOffset { get; private set; }

        public long LineOffset { get; private set; }

        public int ByteCount { get; private set; }

        public int LineCount
        {
            get { return 1; }
        }

        public LineItem(string line, long lineOffset, long byteOffset, int byteCount)
        {
            Line = line;
            LineOffset = lineOffset;
            ByteOffset = byteOffset;
            ByteCount = byteCount;
        }
    }
}
