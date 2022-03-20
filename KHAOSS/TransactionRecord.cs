using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    [MessagePackObject]
    public class TransactionRecord
    {
        [Key(0)]
        public string Key;
        [Key(1)]
        public int Version;
        [Key(2)]
        public byte[] BodyHash;
        [Key(3)]
        public ReadOnlyMemory<byte> Body;
        [Key(4)]
        public DocumentChangeType ChangeType;

        [IgnoreMember]
        public int SizeInStore;

    }
}
