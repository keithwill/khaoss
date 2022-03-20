using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS
{
    /// <summary>
    /// A transaction store that doesn't actually store anything
    /// passed to it. Only useful for testing.
    /// </summary>
    public class NoOpStore : ITransactionStore
    {
        public void Dispose()
        {
        }

        public IEnumerable<TransactionRecord> LoadRecords(CancellationToken cancellationToken = default)
        {
            yield break;
        }

        public void StartFileManagement()
        {
        }

        public void RemoveAllDocuments()
        {
        }

        public void WriteTransaction(Transaction transaction)
        {
            //NoOp
            transaction.SetResult(TransactionResult.Complete);
        }

        public Task ForceMaintenance()
        {
            return Task.CompletedTask;
        }
    }
}
