﻿using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace KHAOSS;

public class Connection<TEntity> : IDisposable where TEntity : class, IEntity
{
    private readonly TransactionQueue<TEntity> transactionQueue;
    private readonly TransactionLog<TEntity> transactionStore;
    private readonly EntityStore<TEntity> memoryStore;

    public Connection(
        TransactionQueue<TEntity> transactionProcessor,
        TransactionLog<TEntity> transactionStore,
        EntityStore<TEntity> memoryStore
        )
    {
        this.transactionQueue = transactionProcessor;
        this.transactionStore = transactionStore;
        this.memoryStore = memoryStore;
    }

    public static Connection<TEntity> Create(string databaseFilePath, JsonTypeInfo<TEntity> jsonTypeInfo)
    {

        var file = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
        var memoryStore = new EntityStore<TEntity>();

        var transactionStore = new TransactionLog<TEntity>(
            file,
            RewriteFileStreamFactory,
            SwapFileRewriteStream,
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

    private static Stream SwapFileRewriteStream(Stream outputStream, Stream rewriteStream)
    {
        var rewriteFileStream = (FileStream)rewriteStream;
        var outputFileStream = (FileStream)outputStream;

        var databaseFilePath = outputFileStream.Name;
        var rewriteFilePath = rewriteFileStream.Name;

        rewriteFileStream.Close();
        outputFileStream.Close();

        File.Move(rewriteFilePath, databaseFilePath, true);
        var newOutputStream = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
        newOutputStream.Position = newOutputStream.Length;
        return newOutputStream;
    }

    private static Stream RewriteFileStreamFactory()
    { 
        return new FileStream(Guid.NewGuid().ToString() + ".tmp", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 65536);
    }

    public static Connection<TEntity> CreateTransient(JsonTypeInfo<TEntity> jsonTypeInfo)
    {
        var memoryStream = new MemoryStream();
        var memoryStore = new EntityStore<TEntity>();
        var transactionStore = new TransactionLog<TEntity>(
            memoryStream,
            () => new MemoryStream(),
            SwapMemoryRewriteStream,
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

    private static Stream SwapMemoryRewriteStream(Stream outputStream, Stream rewriteStream)
    {
        outputStream.Dispose();
        return rewriteStream;
        //var newOutputStream = new MemoryStream();
        //rewriteStream.CopyTo(newOutputStream);
        //newOutputStream.Position = newOutputStream.Length;
        //return newOutputStream;
    }

    public async Task OpenAsync(CancellationToken cancellationToken)
    {
        this.transactionStore.StartFileManagement();
        await this.LoadRecordsFromStore(cancellationToken);
        await this.transactionQueue.Start();
    }

    public async Task Close(CancellationToken cancellationToken)
    {
        this.disposing = true;
        await this.transactionQueue.Stop();
        this.transactionStore.Dispose();
        if (this.transactionStore.RewriteTask != null && !this.transactionStore.RewriteTask.IsCompleted)
        {
            await this.transactionStore.RewriteTask;
        }
    }

    private async Task LoadRecordsFromStore(CancellationToken cancellationToken)
    {
        await foreach (var record in this.transactionStore.LoadRecords(cancellationToken))
        {
            memoryStore.LoadChange(record.Key, record);
        }
    }

    private bool disposing = false;

    public bool IsDisposed => this.disposing;

    public void Dispose()
    {
        if (disposing)
        {
            return; // Already disposing
        }
        disposing = true;
        try
        {
            this.transactionQueue.Stop().Wait();
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
        this.memoryStore.Lock();
        try
        {
            this.memoryStore.RemoveAllDocuments();
            this.transactionStore.RemoveAllDocuments();
        }
        finally
        {
            this.memoryStore.Unlock();
        }
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
        await transactionQueue.Enqueue(transaction);
        return transaction.Entities;
    }

    public async Task<T> Save<T>(T entity) where T : class, TEntity
    {
        var transaction = new Transaction<TEntity>(entity);
        await transactionQueue.Enqueue(transaction);
        return (T)transaction.Entity;
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