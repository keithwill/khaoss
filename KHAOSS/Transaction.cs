using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Transaction
    {
        public DocumentChange DocumentChange;
        public DocumentChange[] DocumentChanges;
        private TaskCompletionSource<TransactionResult> taskCompletionSource = new TaskCompletionSource<TransactionResult>(TaskCreationOptions.RunContinuationsAsynchronously);

        private TransactionResult transactionResult;

        public TransactionResult TransactionResult => transactionResult;
        public bool IsComplete => taskCompletionSource.Task.IsCompleted;

        private string errorMessage = null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            this.DocumentChange = null;
            this.DocumentChanges = null;
            this.taskCompletionSource = null;
            this.errorMessage = null;
            this.taskCompletionSource = new TaskCompletionSource<TransactionResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public string ErrorMessage => errorMessage;
        public Transaction()
        {

        }

        public bool IsSingleChange => this.DocumentChange != null;

        public void SetResult(TransactionResult result, string errorMessage = null)
        {
            this.errorMessage = errorMessage;
            this.transactionResult = result;
            this.taskCompletionSource.SetResult(result);
        }

        public Task<TransactionResult> Task => taskCompletionSource.Task;

    }
}
