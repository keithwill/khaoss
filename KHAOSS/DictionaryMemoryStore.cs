using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class DictionaryMemoryStore : IMemoryStore
    {
        private readonly Dictionary<string, Document> lookup = new Dictionary<string, Document>();

        private long deadSpace;

        public DictionaryMemoryStore()
        {

        }

        public Document Get(string key)
        {
            if (lookup.TryGetValue(key, out var document))
            {
                return document;
            }
            return null;
        }

        public IEnumerable<KeyValuePair<string, Document>> GetByPrefix(string prefix, bool sortResults)
        {
            throw new NotImplementedException();
        }

        public long DeadSpace => deadSpace;

        public void LoadDelete(string key, int version, int sizeInStore)
        {
            var existingRecord = Get(key);
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


        public void LoadSet(string key, Document document)
        {
            var existingRecord = Get(key);
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

        public TransactionResult ProcessTransaction(Transaction transaction)
        {
            if (transaction.IsSingleChange)
            {
                var change = transaction.DocumentChange;
                var existingDocument = Get(change.Key);
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
                    var existingDocument = Get(change.Key);
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

            return TransactionResult.None;
        }

        public void RemoveAllDocuments()
        {
            lookup.Clear();
        }

        public void AddDeadSpace(long additionalDeadSpace)
        {
            this.deadSpace += additionalDeadSpace;
        }

        public void ResetDeadSpace()
        {
            this.deadSpace = 0;
        }
    }
}
