using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Document
    {
        public Document(int version, byte[] body)
        {
            Version = version;
            Body = body;
        }

        public int Version { get; internal set; }
        public byte[] Body { get; internal set; }
        internal int SizeInStore { get; set; }

    }
}
