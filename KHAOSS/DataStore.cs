using System.Linq;

namespace KHAOSS;

/// <summary>
/// The main entry point for the Data Smore storage engine.
/// Passes requests to the transaction processor, and maintains the
/// calling context so that the results can be returned to the caller
/// from a task after the transaction processor finishes processing the request.
/// </summary>
public class DataStore<TBaseEntity> where TBaseEntity : class, IEntity
{
    private readonly TransactionQueueProcessor<TBaseEntity> transactionProcessor;
    private readonly MemoryStore<TBaseEntity> memoryStore;

    public DataStore(TransactionQueueProcessor<TBaseEntity> transactionProcessor, MemoryStore<TBaseEntity> memoryStore)
    {
        this.transactionProcessor = transactionProcessor;
        this.memoryStore = memoryStore;
    }

    public async Task<TBaseEntity[]> Save(TBaseEntity[] entities)
    {
        var transaction = new Transaction<TBaseEntity>(entities);
        await transactionProcessor.ProcessTransaction(transaction);
        return transaction.Entities;
    }

    public async Task<T> Save<T>(T entity) where T : class, TBaseEntity
    {
        var transaction = new Transaction<TBaseEntity>(entity);
        await transactionProcessor.ProcessTransaction(transaction);
        return transaction.Entity as T;
    }

    public T Get<T>(string key) where T : class, TBaseEntity
    {
        return memoryStore.Get(key) as T;
    }

    public IEnumerable<T> GetByPrefix<T>(string prefix, bool sortResults) where T : class, TBaseEntity
    {
        return memoryStore.GetByPrefix(prefix, sortResults).Cast<T>();
    }

}