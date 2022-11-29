using System.Collections.Generic;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface ITransactionProcessor<T> where T : IEntity
    {

        Task ProcessTransaction(Transaction<T> transaction);
        Task<T> ProcessGet(string key);
        Task<IEnumerable<T>> ProcessGetByPrefix(string prefix, bool sortResults);
        Task Start();
        Task Stop();
    }
}