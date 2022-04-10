namespace output2db.esr
{
    public interface IFileChunk
    {
        long ByteOffset { get; }

        long LineOffset { get; }

        int ByteCount { get; }

        int LineCount { get; }
    }
}
