using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS
{
    public interface ITransactionStore : IDisposable
    {

        void WriteTransaction(Transaction transaction);
        void StartFileManagement();

        IAsyncEnumerable<TransactionRecord> LoadRecords(CancellationToken cancellationToken);

        void RemoveAllDocuments();
        Task ForceMaintenance();

    }
}