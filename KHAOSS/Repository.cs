
namespace KHAOSS;
public class Repository : IRepository
{
    private readonly IDataStore dataStore;
    private readonly IEntitySerializer entitySerializer;

    public Repository(IDataStore dataStore, IEntitySerializer entitySerializer)
    {
        this.dataStore = dataStore;
        this.entitySerializer = entitySerializer;
    }

    public async Task ForceSave<T>(string key, T entity)
    {
        var body = entitySerializer.Serialize(entity);
        var saveDocument = new Document(0, body);

        while (true)
        {
            var dbEntity = await dataStore.Get(key);
            saveDocument.Version = dbEntity.Version;
            var result = await dataStore.Set(key, saveDocument);
            if (result == TransactionResult.Complete)
            {
                return;
            }
            if (result != TransactionResult.FailedConcurrencyCheck)
            {
                throw new Exception("Unexpected error while saving entity TransactionResult:" + result.ToString());
            }
        }
    }

    public async Task<Entity<T>> Get<T>(string key)
    {
        var document = await dataStore.Get(key);
        if (document == null)
        {
            return null;
        }
        else
        {
            var value = entitySerializer.Deserialize<T>(document.Body);
            if (value == null)
            {
                return null;
            }
            return new Entity<T>
            {
                Document = document,
                Value = value,
                RetreiveVersion = document.Version
            };

        }
    }

    public async IAsyncEnumerable<Entity<T>> GetByPrefix<T>(string prefix, bool sortResults)
    {
        var results = await dataStore.GetByPrefix(prefix, sortResults);
        foreach (var result in results)
        {
            var value = entitySerializer.Deserialize<T>(result.Value.Body);
            yield return new Entity<T>
            {
                Document = result.Value,
                Value = value,
                Key = result.Key,
                RetreiveVersion = result.Value.Version
            };
        }
    }

    public async IAsyncEnumerable<T> GetByPrefixUntracked<T>(string prefix, bool sortResults)
    {
        var results = await dataStore.GetByPrefix(prefix, sortResults);
        foreach (var result in results)
        {
            var value = entitySerializer.Deserialize<T>(result.Value.Body);
            yield return value;
        }
    }

    public async Task Save<T>(string key, Entity<T> entity)
    {
        var bytes = entitySerializer.Serialize<T>(entity.Value);
        if (entity.RetreiveVersion != entity.Document.Version)
        {
            throw new Exception("Concurrency error. The version that is being saved is older than the current version in the database");
        }
        entity.Document.Body = bytes;
        await dataStore.Set(key, entity.Document);
    }

}

