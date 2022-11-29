// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Text;
// using System.Threading.Tasks;

// namespace KHAOSS
// {
//     ///<summary>
//     ///
//     ///</summary>
//     public class TransactionLockingProcessor : ITransactionProcessor
//     {

//         private readonly ITransactionStore transactionStore;
//         private readonly IMemoryStore memoryStore;
//         private object transactionLock = new object();

//         public TransactionLockingProcessor(
//             ITransactionStore transactionStore,
//             IMemoryStore memoryStore
//         )
//         {
//             this.transactionStore = transactionStore;
//             this.memoryStore = memoryStore;
//         }

//         public Task<Document> ProcessGet(string key)
//         {
//             lock(transactionLock)
//             {
//                 return Task.FromResult(memoryStore.Get(key));
//             }
//         }

//         public Task<IEnumerable<KeyValuePair<string, Document>>> ProcessGetByPrefix(string prefix, bool sortResults)
//         {
//             lock(transactionLock)
//             {
//                 return Task.FromResult(memoryStore.GetByPrefix(prefix, sortResults));
//             }
//         }

//         public Task<TransactionResult> ProcessTransaction(Transaction transaction)
//         {
//             lock(transactionLock)
//             {
                
//                 this.memoryStore.ProcessTransaction(transaction);
//                 if (transaction.IsComplete)
//                 {
//                     return Task.FromResult(TransactionResult.Complete);
//                 }
//                 this.transactionStore.WriteTransaction(transaction);
//                 return Task.FromResult(transaction.TransactionResult);
//             }
//         }

//         public Task Start()
//         {
//             return Task.CompletedTask;
//         }

//         public Task Stop()
//         {
//             return Task.CompletedTask;
//         }
//     }
// }
