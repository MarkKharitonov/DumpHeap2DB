using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Text;

namespace output2db.esr
{
    public class ExtendedStreamReader : IExtendedStreamReader
    {
        public readonly UTF8Encoding Utf8EncodingWoutBom = new UTF8Encoding();

        public IObservable<LineItem> GetLineSource(Stream stream, int bufferSize = Consts.DefBufferSize, Encoding encoding = null, IList<Skip> skips = null)
        {
            if (skips != null)
            {
                if (!stream.CanSeek)
                {
                    throw new NotSupportedException(string.Format("Skips are not supported, because the given stream does not support seeking."));
                }

                for (var i = 1; i < skips.Count; ++i)
                {
                    if (skips[i - 1].NextByteOffset >= skips[i].ByteOffset || skips[i - 1].NextLineOffset >= skips[i].LineOffset)
                    {
                        throw new ArgumentException("The skips collection when given must describe strictly monotonically increasing intervals!", "skips");
                    }
                }
            }

            return Observable.Create<LineItem>(async obs =>
            {
                encoding = encoding ?? Utf8EncodingWoutBom;
                var buffer = new byte[bufferSize];
                int countRead = 0;
                var pos = 0;
                var mayHaveMore = true;
                int nextSkipIndex = 0;
                long lineOffset = 0;
                long byteOffset = 0;
                int desiredByteCount = buffer.Length;
                int charByteCount = encoding.GetByteCount(" ");
                if (charByteCount == 0) charByteCount = 1;

                if (skips != null && skips.Count > 0)
                {
                    if (skips[0].ByteOffset == 0)
                    {
                        lineOffset = skips[0].NextLineOffset;
                        stream.Position = byteOffset = skips[0].NextByteOffset;
                        nextSkipIndex = 1;
                    }
                    if (nextSkipIndex < skips.Count && skips[nextSkipIndex].ByteOffset - byteOffset < desiredByteCount)
                    {
                        desiredByteCount = (int)(skips[nextSkipIndex].ByteOffset - byteOffset);
                    }
                }
                while (mayHaveMore && (countRead = await stream.ReadAsync(buffer, pos, desiredByteCount)) > 0)
                {
                    mayHaveMore = countRead == desiredByteCount;
                    countRead += pos;
                    int i = pos;
                    var found = false;

                    if (pos > 0 && (buffer[pos - 1] == (byte)'\r' || buffer[pos - 1] == (byte)'\n'))
                    {
                        // The previous chunk ends with a newline suspect. Backoff one byte to process the suspected newline sequence from its beginning.
                        --i;
                    }

                    pos = 0;
                    for (; i < countRead; ++i)
                    {
                        if (buffer[i] == (byte)'\r' || buffer[i] == (byte)'\n')
                        {
                            int strLen = i - pos;
                            if (i + charByteCount < countRead)
                            {
                                if (buffer[i] == '\r' && buffer[i + charByteCount] == '\n')
                                {
                                    i += charByteCount;
                                }
                            }
                            else
                            {
                                // The end of the line may be carried over onto the next buffer. We must stop here and get that next buffer.
                                break;
                            }
                            found = true;
                            obs.OnNext(new LineItem(encoding.GetString(buffer, pos, strLen), lineOffset, byteOffset, i - pos + 1));
                            ++lineOffset;
                            byteOffset += i - pos + 1;
                            pos = i + charByteCount;
                        }
                    }
                    if (mayHaveMore)
                    {
                        if (skips != null && nextSkipIndex < skips.Count && byteOffset == skips[nextSkipIndex].ByteOffset)
                        {
                            lineOffset = skips[nextSkipIndex].NextLineOffset;
                            stream.Position = byteOffset = skips[nextSkipIndex].NextByteOffset;
                            ++nextSkipIndex;
                            pos = 0;
                        }
                        else
                        {
                            if (found)
                            {
                                Array.Copy(buffer, pos, buffer, 0, countRead - pos);
                                pos = buffer.Length - pos;
                            }
                            else
                            {
                                var buffer2 = new byte[2 * countRead];
                                Array.Copy(buffer, buffer2, countRead);
                                buffer = buffer2;
                                pos = countRead;
                            }
                            if (stream.CanSeek && stream.Position != byteOffset + pos)
                            {
                                throw new Exception(string.Format("Current stream.Position value {0} does not match the expected value of {1}!", stream.Position, byteOffset + pos));
                            }
                        }
                        desiredByteCount = buffer.Length - pos;
                        if (skips != null && nextSkipIndex < skips.Count && skips[nextSkipIndex].ByteOffset - stream.Position < desiredByteCount)
                        {
                            desiredByteCount = (int)(skips[nextSkipIndex].ByteOffset - stream.Position);
                        }
                    }
                }
                if (pos < countRead)
                {
                    obs.OnNext(new LineItem(encoding.GetString(buffer, pos, countRead - pos), lineOffset, byteOffset, countRead - pos));
                }
            });
        }
    }
}
