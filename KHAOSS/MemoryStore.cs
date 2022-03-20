using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    /// <summary>
    /// An light in-memory transactional wrapper around storing documents in memory.
    /// The in-memory representation is implemented as a radix tree.
    /// </summary>
    public class MemoryStore : IMemoryStore
    {

        private readonly PrefixLookup<Document> lookup;

        private long deadSpace;
        public long DeadSpace => deadSpace;

        public MemoryStore()
        {
            lookup = new PrefixLookup<Document>();
        }

        public void Set(string key, Document document)
        {
            lookup.Add(key, document);
        }

        public Document Get(string key)
        {
            return lookup.Get(key);
        }

        public IEnumerable<KeyValuePair<string, Document>> GetByPrefix(string prefix, bool sortResults)
        {
            return lookup.GetByPrefix(prefix, sortResults);
        }

        public TransactionResult ProcessTransaction(Transaction transaction)
        {

            if (transaction.IsSingleChange)
            {
                var change = transaction.DocumentChange;
                var existingDocument = lookup.Get(change.Key);
                if (existingDocument != null)
                {
                    if (existingDocument.Version > change.Version)
                    {
                        transaction.SetResult(TransactionResult.FailedConcurrencyCheck);
                        return TransactionResult.FailedConcurrencyCheck;
                    }
                }

                change.PassedConcurrencyCheck();
                if (existingDocument != null)
                {
                    deadSpace += existingDocument.SizeInStore;
                }

                switch (change.ChangeType)
                {
                    case DocumentChangeType.Set:
                        lookup.Add(change.Key, change.Document);
                        break;
                    case DocumentChangeType.Delete:
                        lookup.Remove(change.Key);
                        
                        break;
                    default:
                        transaction.SetResult(TransactionResult.UnexpectedError, $"Unknown and unhandled change type detected: {change.ChangeType}");
                        return TransactionResult.UnexpectedError;
                }
            }
            else
            {
                int deadSpaceInExistingDocuments = 0;
                for (int i = 0; i < transaction.DocumentChanges.Length; i++)
                {
                    DocumentChange change = transaction.DocumentChanges[i];
                    var existingDocument = lookup.Get(change.Key);
                    if (existingDocument != null)
                    {
                        if (existingDocument.Version != change.Version)
                        {
                            transaction.SetResult(TransactionResult.FailedConcurrencyCheck);
                            return TransactionResult.FailedConcurrencyCheck;
                        }
                        deadSpaceInExistingDocuments += existingDocument.SizeInStore;
                    }
                }

                this.deadSpace += deadSpaceInExistingDocuments;

                for (int i = 0; i < transaction.DocumentChanges.Length; i++)
                {
                    DocumentChange change = transaction.DocumentChanges[i];
                    change.PassedConcurrencyCheck();

                    switch (change.ChangeType)
                    {
                        case DocumentChangeType.Set:
                            lookup.Add(change.Key, change.Document);
                            break;
                        case DocumentChangeType.Delete:
                            lookup.Remove(change.Key);
                            break;
                        default:
                            transaction.SetResult(TransactionResult.UnexpectedError, $"Unknown and unhandled change type detected: {change.ChangeType}");
                            return TransactionResult.UnexpectedError;
                    }
                }
            }

            return TransactionResult.Complete;
        }

        public void LoadSet(string key, Document document)
        {
            var existingRecord = lookup.Get(key);
            if (existingRecord != null)
            {
                if (document.Version > existingRecord.Version)
                {
                    lookup.Add(key, document);
                    deadSpace += existingRecord.SizeInStore;
                }
                else
                {
                    deadSpace += document.SizeInStore;
                }
            }
            else
            {
                lookup.Add(key, document);
            }
        }

        public void LoadDelete(string key, int version, int sizeInStore)
        {

            var existingRecord = lookup.Get(key);
            if (existingRecord != null)
            {
                if (version > existingRecord.Version)
                {
                    lookup.Remove(key);
                    deadSpace += existingRecord.SizeInStore;
                }
            }
            // The document is stored as well as the transaction to later delete it
            // They both add to the storage size
            deadSpace += sizeInStore;

        }

        public void RemoveAllDocuments()
        {
            lookup.Clear();
        }

        public void AddDeadSpace(long additionalDeadSpace)
        {
            deadSpace += additionalDeadSpace;
        }

        public void ResetDeadSpace()
        {
            deadSpace = 0;
        }

    }
}
