using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class DocumentChange
    {
        public string Key { get; set; }
        public Document Document { get; set; }
        public DocumentChangeType ChangeType { get; set; }
        public int Version => ChangeType == DocumentChangeType.Delete ? deleteVersion : Document.Version;

        private int deleteVersion = 0;

        public DocumentChange()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AsSetChange(string key, Document document)
        {
            Key = key;
            Document = document;
            ChangeType = DocumentChangeType.Set;
        }

        public void AsDeleteChange(string key, int version)
        {
            Key = key;
            deleteVersion = version;
            ChangeType = DocumentChangeType.Delete;
        }

        public void PassedConcurrencyCheck()
        {
            if (ChangeType == DocumentChangeType.Set)
            {
                Document.Version += 1;
            }
            else if (ChangeType == DocumentChangeType.Delete)
            {
                deleteVersion += 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            this.Key = null;
            this.Document = null;
            this.ChangeType = DocumentChangeType.Unknown;
            this.deleteVersion = 0;
        }
    }
}
