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
    private readonly Channel<GetCorrelation> getChannel;
    private readonly Channel<Transaction> transactionChannel;
    private readonly Channel<GetByPrefixCorrelation> getByPrefixChannel;


    public TransactionQueueProcessor(
        ITransactionStore transactionStore,
        IMemoryStore memoryStore
    )
    {
        this.getChannel = Channel.CreateUnbounded<GetCorrelation>(new UnboundedChannelOptions { SingleReader = true });
        this.transactionChannel = Channel.CreateUnbounded<Transaction>(new UnboundedChannelOptions { SingleReader = true });
        this.getByPrefixChannel = Channel.CreateUnbounded<GetByPrefixCorrelation>(new UnboundedChannelOptions { SingleReader = true });
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
        this.processQueuesTask = Task.Run(() => ProcessQueues());
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

    private void ProcessQueues()
    {
        Stopwatch lastActiveTimer = new Stopwatch();
        lastActiveTimer.Start();

        var cancellationToken = processQueuesCancellationTokenSource.Token;
        while (cancellationToken.IsCancellationRequested == false)
        {
            int processed = 0;
            while (getChannel.Reader.TryRead(out var getCorrelation))
            {
                var document = memoryStore.Get(getCorrelation.Key);
                getCorrelation.SetResult(document);
                processed++;
            }

            if (transactionChannel.Reader.TryRead(out var transaction))
            {
                var transactionResult = memoryStore.ProcessTransaction(transaction);
                if (transactionResult == TransactionResult.Complete)
                {
                    transactionStore.WriteTransaction(transaction);
                }
                processed++;
            }

            if (getByPrefixChannel.Reader.TryRead(out var prefixCorrelation))
            {
                var prefixResult = memoryStore.GetByPrefix(prefixCorrelation.Prefix, prefixCorrelation.SortResults);
                prefixCorrelation.SetResult(prefixResult);
                processed++;
            }

            if (processed == 0)
            {
                if (lastActiveTimer.ElapsedMilliseconds > 200)
                {
                    WaitForQueuedItems(cancellationToken);
                }
            }
            else
            {
                lastActiveTimer.Restart();
            }

        }
    }

    private readonly static Task[] waitTasks = new Task[3];


    private void WaitForQueuedItems(CancellationToken cancellationToken)
    {
        //https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/#combinators
        var waitCancellation = new CancellationTokenSource();
        waitTasks[0] = getChannel.Reader.WaitToReadAsync(waitCancellation.Token).AsTask();
        waitTasks[1] = getByPrefixChannel.Reader.WaitToReadAsync(waitCancellation.Token).AsTask();
        waitTasks[2] = transactionChannel.Reader.WaitToReadAsync(waitCancellation.Token).AsTask();
        try
        {
            Task.WaitAny(waitTasks, cancellationToken);
            waitCancellation.Cancel();
        }
        catch (OperationCanceledException)
        {
        }
    }

    public Task<Document> ProcessGet(string key)
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
        return correlation.Task;
    }

    public Task<IEnumerable<KeyValuePair<string, Document>>> ProcessGetByPrefix(string prefix, bool sortResults)
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
        return correlation.Task;
    }

    public Task<TransactionResult> ProcessTransaction(Transaction transaction)
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
        return transaction.Task;
    }
}
