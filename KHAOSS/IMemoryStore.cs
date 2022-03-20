using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IMemoryStore
    {
        Document Get(string key);
        IEnumerable<KeyValuePair<string, Document>> GetByPrefix(string prefix, bool sortResults);
        TransactionResult ProcessTransaction(Transaction transaction);
        void LoadSet(string key, Document document);
        void LoadDelete(string key, int version, int sizeInStore);
        long DeadSpace { get; }
        void AddDeadSpace(long additionalDeadSpace);
        void RemoveAllDocuments();
        void ResetDeadSpace();
    }
}
