using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface ITransactionStore<T> : IDisposable where T : IEntity
    {

        void WriteTransaction(Transaction<T> transaction);
        void StartFileManagement();

        IEnumerable<T> LoadRecords(CancellationToken cancellationToken);

        void RemoveAllDocuments();
        Task ForceMaintenance();
        Task RewriteTask { get; }

    }
}