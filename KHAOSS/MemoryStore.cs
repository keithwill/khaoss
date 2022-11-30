﻿namespace KHAOSS;

/// <summary>
/// An light in-memory transactional wrapper around storing documents in memory.
/// The in-memory representation is implemented as a radix tree.
/// </summary>
public class MemoryStore<T> where T : class, IEntity
{

    private readonly PrefixLookup<T> lookup;

    private long deadEntityCount;
    private long entityCount;
    public long DeadEntityCount => deadEntityCount;
    public long EntityCount => entityCount;
    public MemoryStore()
    {
        lookup = new();
    }

    public T Get(string key)
    {
        var result = lookup.Get(key);
        return result.Deleted ? default(T) : result;
    }

    public IEnumerable<T> GetByPrefix(string prefix, bool sortResults)
    {
        if (sortResults)
        {
            return lookup.GetKeyValuePairByPrefix(prefix, sortResults)
                .Where(x => !x.Value.Deleted)
                .Select(x => x.Value);
        }
        else
        {
            return lookup.GetByPrefixValues(prefix);
        }
    }

    public bool ProcessTransaction(Transaction<T> transaction)
    {

        if (transaction.IsSingleChange)
        {
            var entity = transaction.Entity;
            var existingDocument = lookup.Get(entity.Key);
            if (existingDocument != null)
            {

                if (
                    (existingDocument.Version > entity.Version) ||
                    (existingDocument.Deleted && entity.Version != 0)
                    )
                {
                    transaction.Entity = existingDocument;
                    transaction.SetError(new OptimisticConcurrencyException(entity.Key, entity.Version, existingDocument.Version));
                    return false;
                }
                deadEntityCount++;
            }
            else
            {
                entityCount++;
            }

            entity = (T)entity.WithVersion(entity.Version + 1);
            lookup.Add(entity.Key, entity);

            transaction.Entity = entity;
        }
        else
        {
            int deadEntitiesInExisting = 0;
            int newEntitiesCount = 0;
            for (int i = 0; i < transaction.Entities.Length; i++)
            {
                var entity = transaction.Entities[i];
                var existingDocument = lookup.Get(entity.Key);
                if (existingDocument != null)
                {
                    if (
                        (existingDocument.Version > entity.Version) ||
                        (existingDocument.Deleted && entity.Version != 0)
                        )
                    {
                        transaction.Entity = existingDocument;
                        transaction.SetError(new OptimisticConcurrencyException(entity.Key, entity.Version, existingDocument.Version));
                        return false;
                    }
                    deadEntitiesInExisting++;
                }
                else
                {
                    newEntitiesCount++;
                }
            }

            this.deadEntityCount += deadEntitiesInExisting;
            this.entityCount += newEntitiesCount;

            for (int i = 0; i < transaction.Entities.Length; i++)
            {
                var change = transaction.Entities[i];
                change = (T)change.WithVersion(change.Version + 1);
                lookup.Add(change.Key, change);
                transaction.Entities[i] = change;
            }
        }

        return true;
    }

    public void LoadChange(string key, T entity)
    {
        var existingRecord = lookup.Get(key);
        if (existingRecord != null)
        {
            if (entity.Version > existingRecord.Version)
            {
                lookup.Add(key, entity);
            }
            deadEntityCount++;
        }
        else
        {
            entityCount++;
            lookup.Add(key, entity);
        }
    }


    public void RemoveAllDocuments()
    {
        lookup.Clear();
    }

    public void IncrementDeadEntities()
    {
        deadEntityCount++;
    }

    public void ResetDeadSpace()
    {
        deadEntityCount = 0;
    }

}
