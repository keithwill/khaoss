using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Document
    {
        public int Version { get; set; }
        public ReadOnlyMemory<byte> Body { get; set; }
        public int SizeInStore { get; set; }

    }
}
