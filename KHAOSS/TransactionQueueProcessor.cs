using System.Diagnostics;
using System.Threading.Channels;

namespace KHAOSS;

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
    private readonly Channel<QueueItem> queueItemChannel;
    private readonly NaiveObjectPool<QueueItem> queueItemPool = new();


    public TransactionQueueProcessor(
        ITransactionStore transactionStore,
        IMemoryStore memoryStore
    )
    {
        this.queueItemChannel = Channel.CreateUnbounded<QueueItem>(new UnboundedChannelOptions { SingleReader = true });
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
        this.processQueuesTask = Task.Run(async () => await ProcessQueues());
        return Task.CompletedTask;
    }

    public async Task Stop()
    {
            this.processQueuesCancellationTokenSource.Cancel();
            this.queueItemChannel.Writer.Complete();
            await processQueuesTask;
    }

    private async Task ProcessQueues()
    {

        var cancellationToken = processQueuesCancellationTokenSource.Token;

        try
        {
            await foreach (var item in queueItemChannel.Reader.ReadAllAsync(cancellationToken))
            {
                if (item.GetCorrelation != null)
                {
                    var document = memoryStore.Get(item.GetCorrelation.Key);
                    item.GetCorrelation.SetResult(document);
                }
                else if (item.Transaction != null)
                {
                    var transactionResult = memoryStore.ProcessTransaction(item.Transaction);
                    if (transactionResult == TransactionResult.Complete)
                    {
                        transactionStore.WriteTransaction(item.Transaction);
                    }
                }
                else if (item.GetByPrefixCorrelation != null)
                {
                    var prefixResult = memoryStore.GetByPrefix(item.GetByPrefixCorrelation.Prefix, item.GetByPrefixCorrelation.SortResults);
                    item.GetByPrefixCorrelation.SetResult(prefixResult);
                }
            }
        } 
        catch (OperationCanceledException)
        {
        }

    }

    public Task<Document> ProcessGet(string key)
    {
        var correlation = new GetCorrelation(key);
        var queueItem = new QueueItem { GetCorrelation = correlation };
        if (!queueItemChannel.Writer.TryWrite(queueItem))
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
        return correlation.Task;
    }

    public Task<IEnumerable<KeyValuePair<string, Document>>> ProcessGetByPrefix(string prefix, bool sortResults)
    {
        var correlation = new GetByPrefixCorrelation(prefix, sortResults);
        var queueItem = new QueueItem { GetByPrefixCorrelation = correlation };
        //var queueItem = queueItemPool.Lease();
        //queueItem.Reset();
        //queueItem.GetByPrefixCorrelation = correlation;
        if (!queueItemChannel.Writer.TryWrite(queueItem))
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
        return correlation.Task;
        //queueItemPool.Return(queueItem);
        //return result;
    }

    public Task<TransactionResult> ProcessTransaction(Transaction transaction)
    {
        var queueItem = new QueueItem();
        queueItem.Transaction = transaction;

        if (!queueItemChannel.Writer.TryWrite(queueItem))
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
        return transaction.Task;
    }
}
