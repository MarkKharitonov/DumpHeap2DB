using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace output2db.esr
{
    public interface IExtendedStreamReader
    {
        IObservable<LineItem> GetLineSource(Stream stream, int bufferSize = Consts.DefBufferSize, Encoding encoding = null, IList<Skip> skips = null);
    }
}
