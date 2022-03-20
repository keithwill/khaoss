using System.Collections.Generic;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface ITransactionProcessor
    {

        Task<TransactionResult> ProcessTransaction(Transaction transaction);
        Task<Document> ProcessGet(string key);
        Task<IEnumerable<KeyValuePair<string, Document>>> ProcessGetByPrefix(string prefix, bool sortResults);
        Task Start();
        Task Stop();
    }
}