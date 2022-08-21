using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface IDataStore
    {
        Task<Document> Get(string key);
        Task<TransactionResult> Set(string key, Document document);
        Task<TransactionResult> Set(string key, byte[] body, int version);
        Task<IEnumerable<KeyValuePair<string, Document>>> GetByPrefix(string prefix, bool sortResults);
        Task<TransactionResult> Remove(string key, int version);
        Task<TransactionResult> Multi(DocumentChange[] changes);
    }
}
