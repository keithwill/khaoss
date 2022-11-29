using System.Diagnostics;
using System.Threading.Channels;

namespace KHAOSS;

/// <summary>
/// Takes requests from the data store and flattens them to a single
/// thread by forcing them to pass through a thread safe queue with a single consumer.
/// Responsible for processing each transaction and passing the results to both
/// any memory cache and transaction store.
/// </summary>
public class TransactionQueueProcessor<TBaseType> : ITransactionProcessor<TBaseType> where TBaseType : IEntity
{
    private readonly ITransactionStore<TBaseType> transactionStore;
    private readonly IMemoryStore<TBaseType> memoryStore;
    private Task processQueuesTask;
    private CancellationTokenSource processQueuesCancellationTokenSource;
    private readonly Channel<QueueItem<TBaseType>> queueItemChannel;
    private readonly NaiveObjectPool<QueueItem<TBaseType>> queueItemPool = new();


    public TransactionQueueProcessor(
        ITransactionStore<TBaseType> transactionStore,
        IMemoryStore<TBaseType> memoryStore
    )
    {
        this.queueItemChannel = Channel.CreateUnbounded<QueueItem<TBaseType>>(new UnboundedChannelOptions { SingleReader = true });
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
                    var appliedInMemory = memoryStore.ProcessTransaction(item.Transaction);

                    if (appliedInMemory)
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

    public Task<TBaseType> ProcessGet(string key)
    {
        var correlation = new GetCorrelation<TBaseType>(key);
        var queueItem = new QueueItem<TBaseType> { GetCorrelation = correlation };
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

    public Task<IEnumerable<TBaseType>> ProcessGetByPrefix(string prefix, bool sortResults)
    {
        var correlation = new GetByPrefixCorrelation<TBaseType>(prefix, sortResults);
        var queueItem = new QueueItem<TBaseType> { GetByPrefixCorrelation = correlation };
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

    public Task ProcessTransaction(Transaction<TBaseType> transaction)
    {
        var queueItem = new QueueItem<TBaseType>();
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
