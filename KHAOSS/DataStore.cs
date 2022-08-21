namespace KHAOSS;

/// <summary>
/// The main entry point for the Data Smore storage engine.
/// Passes requests to the transaction processor, and maintains the
/// calling context so that the results can be returned to the caller
/// from a task after the transaction processor finishes processing the request.
/// </summary>
public class DataStore : IDataStore
{
    private readonly ITransactionProcessor transactionProcessor;

    public DataStore(ITransactionProcessor transactionProcessor)
    {
        this.transactionProcessor = transactionProcessor;
    }

    public Task<TransactionResult> Multi(DocumentChange[] changes)
    {
        var transaction = new Transaction();
        transaction.DocumentChanges = changes;
        return transactionProcessor.ProcessTransaction(transaction);
    }

    public Task<TransactionResult> Set(string key, Document document)
    {

        var transaction = new Transaction();
        var documentChange = new DocumentChange();
        documentChange.AsSetChange(key, document);
        transaction.DocumentChange = documentChange;
        return transactionProcessor.ProcessTransaction(transaction);

    }

    public Task<TransactionResult> Set(string key, byte[] body, int version)
    {
        var document = new Document();
        document.Body = body;
        document.Version = version;
        document.SizeInStore = 0;

        var transaction = new Transaction();
        var documentChange = new DocumentChange();

        documentChange.AsSetChange(key, document);
        transaction.DocumentChange = documentChange;

        return transactionProcessor.ProcessTransaction(transaction);

    }

    public Task<Document> Get(string key)
    {
        return transactionProcessor.ProcessGet(key);
    }

    public Task<IEnumerable<KeyValuePair<string, Document>>> GetByPrefix(string prefix, bool sortResults)
    {
        return transactionProcessor.ProcessGetByPrefix(prefix, sortResults);
    }

    public Task<TransactionResult> Remove(string key, int version)
    {
        var transaction = new Transaction();
        var documentChange = new DocumentChange();

        documentChange.AsDeleteChange(key, version);
        transaction.DocumentChange = documentChange;

        return transactionProcessor.ProcessTransaction(transaction);

    }
}