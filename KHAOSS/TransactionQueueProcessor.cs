using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KHAOSS
{
    /// <summary>
    /// Takes requests from the data store and flattens them to a single
    /// thread by forcing them to pass through a thread safe queue with a single consumer.
    /// Responsible for processing each transaction and passing the results to both
    /// any memory cache and transaction store.
    /// </summary>
    public class TransactionQueueProcessor : ITransactionProcessor
    {
        private readonly ITransactionStore transactionStore;
        private readonly IMemoryStore memoryStore;
        private Task processQueuesTask;
        private CancellationTokenSource processQueuesCancellationTokenSource;
        private readonly Channel<GetCorrelation> getChannel;
        private readonly Channel<Transaction> transactionChannel;
        private readonly Channel<GetByPrefixCorrelation> getByPrefixChannel;

        public TransactionQueueProcessor(
            ITransactionStore transactionStore,
            IMemoryStore memoryStore
        )
        {
            this.getChannel = Channel.CreateUnbounded<GetCorrelation>();
            this.transactionChannel = Channel.CreateUnbounded<Transaction>();
            this.getByPrefixChannel = Channel.CreateUnbounded<GetByPrefixCorrelation>();
            this.transactionStore = transactionStore;
            this.memoryStore = memoryStore;
        }

        public Task Start()
        {
            if (this.processQueuesTask != null)
            {
                throw new InvalidOperationException("Already processing transactions");
            }
            this.processQueuesCancellationTokenSource = new CancellationTokenSource();
            this.processQueuesTask = Task.Run( () => ProcessQueues(processQueuesCancellationTokenSource.Token));
            return Task.CompletedTask;
        }

        public async Task Stop()
        {
            this.processQueuesCancellationTokenSource.Cancel();
            this.transactionChannel.Writer.Complete();
            this.getByPrefixChannel.Writer.Complete();
            this.getChannel.Writer.Complete();
            await processQueuesTask;
        }

        private void ProcessQueues(CancellationToken cancellationToken)
        {
            while(cancellationToken.IsCancellationRequested == false)
            {
                var processed = 0;

                processed = ProcessGetRequests(cancellationToken);
                processed += ProcessTransactions(cancellationToken);
                processed += ProcessGetByPrefixRequests(cancellationToken);
                
                if (processed == 0)
                {
                    try
                    {
                        WaitForQueuedItems(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }
            }
        }

        private readonly static Task[] waitTasks = new Task[3];
        private void WaitForQueuedItems(CancellationToken cancellationToken)
        {
            //https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/#combinators
            waitTasks[0] = getChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
            waitTasks[1] = getByPrefixChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
            waitTasks[2] = transactionChannel.Reader.WaitToReadAsync(cancellationToken).AsTask();
            try
            {
                Task.WaitAny(waitTasks, cancellationToken);
            }
            catch (OperationCanceledException)
            {
            }
        }

        private int ProcessGetByPrefixRequests(CancellationToken cancellationToken)
        {
            var processed = 0;
            while (cancellationToken.IsCancellationRequested == false &&
                getByPrefixChannel.Reader.TryRead(out var prefixCorrelation))
            {
                processed++;
                var prefixResult = memoryStore.GetByPrefix(prefixCorrelation.Prefix, prefixCorrelation.SortResults);
                prefixCorrelation.SetResult(prefixResult);
                processed += ProcessGetRequests(cancellationToken);
            }
            return processed;
        }

        private int ProcessTransactions(CancellationToken cancellationToken)
        {
            var processed = 0;
            while ( cancellationToken.IsCancellationRequested == false &&
                transactionChannel.Reader.TryRead(out var transaction)
                )
            {
                processed++;
                var transactionResult = memoryStore.ProcessTransaction(transaction);
                if (transactionResult == TransactionResult.Complete)
                {
                    transactionStore.WriteTransaction(transaction);
                }
                processed += ProcessGetRequests(cancellationToken);
            }
            return processed;
        }

        private int ProcessGetRequests(CancellationToken cancellationToken)
        {

            var processed = 0;
            while(
                cancellationToken.IsCancellationRequested == false  &&
                getChannel.Reader.TryRead(out var getCorrelation))
            {
                processed++;
                var document = memoryStore.Get(getCorrelation.Key);
                getCorrelation.SetResult(document);
            }
            return processed;
        }

        public async Task<Document> ProcessGet(string key)
        {
            var correlation = new GetCorrelation(key);
            if (!getChannel.Writer.TryWrite(correlation))
            {
                if (processQueuesCancellationTokenSource.IsCancellationRequested)
                {
                    throw new Exception("Get request can't complete because Transaction processor is shutting down");
                }
                else
                {
                    // Should never happen. TryWrite is always supposed to succeed
                    // when channel is open and was created unbound
                    throw new Exception("Could not write to transaction processor get queue for an unknown reason");
                }
            }
            return await correlation.Task;
        }

        public async Task<IEnumerable<KeyValuePair<string, Document>>> ProcessGetByPrefix(string prefix, bool sortResults)
        {
            var correlation = new GetByPrefixCorrelation(prefix, sortResults);
            if (!getByPrefixChannel.Writer.TryWrite(correlation))
            {
                if (processQueuesCancellationTokenSource.IsCancellationRequested)
                {
                    throw new Exception("Get by prefix request can't complete because Transaction processor is shutting down");
                }
                else
                {
                    // Should never happen. TryWrite is always supposed to succeed
                    // when channel is open and was created unbound
                    throw new Exception("Could not write to transaction processor get queue for an unknown reason");
                }
            }
            return await correlation.Task;
        }

        public async Task<TransactionResult> ProcessTransaction(Transaction transaction)
        {
            if (!transactionChannel.Writer.TryWrite(transaction))
            {
                if (processQueuesCancellationTokenSource.IsCancellationRequested)
                {
                    throw new Exception("Get by prefix request can't complete because Transaction processor is shutting down");
                }
                else
                {
                    // Should never happen. TryWrite is always supposed to succeed
                    // when channel is open and was created unbound
                    throw new Exception("Could not write to transaction processor get queue for an unknown reason");
                }
            }
            await transaction.Task;
            return transaction.TransactionResult;
        }
    }
}
