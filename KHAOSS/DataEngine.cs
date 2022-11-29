using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace KHAOSS;

public class DataEngine<TEntity> : IDataEngine<TEntity>, IDisposable where TEntity : class, IEntity
{
    private readonly ITransactionProcessor<TEntity> transactionProcessor;
    private readonly ITransactionStore<TEntity> transactionStore;
    private readonly IMemoryStore<TEntity> memoryStore;
    private readonly IDataStore<TEntity> store;

    public DataEngine(
        ITransactionProcessor<TEntity> transactionProcessor,
        ITransactionStore<TEntity> transactionStore,
        IMemoryStore<TEntity> memoryStore
        )
    {
        this.transactionProcessor = transactionProcessor;
        this.transactionStore = transactionStore;
        this.memoryStore = memoryStore;
        this.store = new DataStore<TEntity>(transactionProcessor);
    }

    public static DataEngine<TEntity> Create(string databaseFilePath, JsonTypeInfo<TEntity> jsonTypeInfo)
    {
        var file = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
        var memoryStore = new MemoryStore<TEntity>();
        var transactionStore = new AppendOnlyStore<TEntity>(
            file,
            () => new FileStream(Guid.NewGuid().ToString() + ".tmp", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536),
            (outputStream, rewriteStream) =>
            {
                var rewriteFileStream = (FileStream)rewriteStream;
                var rewriteFilePath = rewriteFileStream.Name;

                rewriteFileStream.Close();
                outputStream.Close();

                File.Replace(rewriteFilePath, databaseFilePath, null);
                var newOutputStream = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
                newOutputStream.Position = newOutputStream.Length;
                newOutputStream.Flush();

                return newOutputStream;
            },
            memoryStore,
            jsonTypeInfo
            );
        var transactionProcessor = new TransactionQueueProcessor<TEntity>(transactionStore, memoryStore);
        return new DataEngine<TEntity>(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public static DataEngine<TEntity> CreateTransient(JsonTypeInfo<TEntity> jsonTypeInfo)
    {
        var memoryStream = new MemoryStream();
        var memoryStore = new MemoryStore<TEntity>();
        var transactionStore = new AppendOnlyStore<TEntity>(
            memoryStream,
            () => new MemoryStream(),
            (outputStream, rewriteStream) =>
            {
                var newOutputStream = new MemoryStream();
                rewriteStream.CopyTo(newOutputStream);
                newOutputStream.Position = newOutputStream.Length;

                return newOutputStream;
            },
            memoryStore,
            jsonTypeInfo
        );
        var transactionProcessor = new TransactionQueueProcessor<TEntity>(transactionStore, memoryStore);
        return new DataEngine<TEntity>(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public IDataStore<TEntity> Store => store;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        this.transactionStore.StartFileManagement();
        this.LoadRecordsFromStore(cancellationToken);
        await this.transactionProcessor.Start();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        this.disposing = true;
        await this.transactionProcessor.Stop();
        this.transactionStore.Dispose();
    }

    private void LoadRecordsFromStore(CancellationToken cancellationToken)
    {
        //Stopwatch sw = new();
        //sw.Start();
        foreach (var record in this.transactionStore.LoadRecords(cancellationToken))
        {
            memoryStore.LoadChange(record.Key, record);
        }
        //sw.Stop();
        //Console.WriteLine($"Finished loading from store: took {sw.Elapsed.TotalSeconds}");
    }

    private bool disposing = false;

    public void Dispose()
    {
        if (disposing)
        {
            return; // Already disposing
        }
        disposing = true;
        try
        {
            this.transactionProcessor.Stop().Wait();
        }
        catch(OperationCanceledException)
        {
        }
        catch(AggregateException)
        {

        }

        try
        {
            this.transactionStore.Dispose();
        }
        catch(OperationCanceledException)
        {

        }
        catch (AggregateException)
        {

        }
    }

    public void RemoveAllDocuments()
    {
        this.memoryStore.RemoveAllDocuments();
        this.transactionStore.RemoveAllDocuments();
    }

    public Task ForceMaintenance()
    {
        return transactionStore.ForceMaintenance();
    }

    public long DeadEntityCount => memoryStore.DeadEntityCount;

    public long EntityCount => memoryStore.EntityCount;

    public double DeadSpacePercentage => memoryStore.EntityCount == 0 ?
        0.0 :
        memoryStore.DeadEntityCount / memoryStore.EntityCount;
}