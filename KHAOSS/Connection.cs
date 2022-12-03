using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace KHAOSS;

public class Connection<TEntity> : IDisposable where TEntity : class, IEntity
{
    private readonly TransactionQueue<TEntity> transactionProcessor;
    private readonly TransactionLog<TEntity> transactionStore;
    private readonly EntityStore<TEntity> memoryStore;

    public Connection(
        TransactionQueue<TEntity> transactionProcessor,
        TransactionLog<TEntity> transactionStore,
        EntityStore<TEntity> memoryStore
        )
    {
        this.transactionProcessor = transactionProcessor;
        this.transactionStore = transactionStore;
        this.memoryStore = memoryStore;
    }

    public static Connection<TEntity> Create(string databaseFilePath, JsonTypeInfo<TEntity> jsonTypeInfo)
    {
        var file = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
        var memoryStore = new EntityStore<TEntity>();
        var transactionStore = new TransactionLog<TEntity>(
            file,
            () => new FileStream(Guid.NewGuid().ToString() + ".tmp", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536),
            (outputStream, rewriteStream) =>
            {
                var rewriteFileStream = (FileStream)rewriteStream;
                var rewriteFilePath = rewriteFileStream.Name;

                rewriteFileStream.Close();
                outputStream.Close();

                File.Move(rewriteFilePath, databaseFilePath, true);
                var newOutputStream = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
                newOutputStream.Position = newOutputStream.Length;
                newOutputStream.Flush();

                return newOutputStream;
            },
            memoryStore,
            jsonTypeInfo
            );
        var transactionProcessor = new TransactionQueue<TEntity>(transactionStore, memoryStore);
        return new Connection<TEntity>(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public static Connection<TEntity> CreateTransient(JsonTypeInfo<TEntity> jsonTypeInfo)
    {
        var memoryStream = new MemoryStream();
        var memoryStore = new EntityStore<TEntity>();
        var transactionStore = new TransactionLog<TEntity>(
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
        var transactionProcessor = new TransactionQueue<TEntity>(transactionStore, memoryStore);
        return new Connection<TEntity>(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public async Task OpenAsync(CancellationToken cancellationToken)
    {
        this.transactionStore.StartFileManagement();
        this.LoadRecordsFromStore(cancellationToken);
        await this.transactionProcessor.Start();
    }

    public async Task Close(CancellationToken cancellationToken)
    {
        this.disposing = true;
        await this.transactionProcessor.Stop();
        this.transactionStore.Dispose();
        if (this.transactionStore.RewriteTask != null && !this.transactionStore.RewriteTask.IsCompleted)
        {
            await this.transactionStore.RewriteTask;
        }
    }

    private void LoadRecordsFromStore(CancellationToken cancellationToken)
    {
        foreach (var record in this.transactionStore.LoadRecords(cancellationToken))
        {
            memoryStore.LoadChange(record.Key, record);
        }
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


    public async Task<TEntity[]> Save(TEntity[] entities)
    {
        var transaction = new Transaction<TEntity>(entities);
        await transactionProcessor.Enqueue(transaction);
        return transaction.Entities;
    }

    public async Task<T> Save<T>(T entity) where T : class, TEntity
    {
        var transaction = new Transaction<TEntity>(entity);
        await transactionProcessor.Enqueue(transaction);
        return transaction.Entity as T;
    }

    public T Get<T>(string key) where T : class, TEntity
    {
        return memoryStore.Get(key) as T;
    }

    public IEnumerable<T> GetByPrefix<T>(string prefix, bool sortResults) where T : class, TEntity
    {
        return memoryStore.GetByPrefix<T>(prefix, sortResults);
    }
}