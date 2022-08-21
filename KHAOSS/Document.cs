﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Document
    {
        public int Version { get; set; }
        public byte[] Body { get; set; }
        public int SizeInStore { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            Version = 0;
            Body = null;
            SizeInStore = 0;
        }
    }
}
