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

    public async Task<TransactionResult> Multi(DocumentChange[] changes)
    {
        var transaction = new Transaction();
        transaction.DocumentChanges = changes;
        var result = await transactionProcessor.ProcessTransaction(transaction);
        return result;
    }

    public async Task<TransactionResult> Set(string key, Document document)
    {

        var transaction = new Transaction();
        var documentChange = new DocumentChange();

        documentChange.AsSetChange(key, document);
        transaction.DocumentChange = documentChange;

        var result = await transactionProcessor.ProcessTransaction(transaction);

        return result;
    }

    public async Task<Document> Get(string key)
    {
        return await transactionProcessor.ProcessGet(key);
    }

    public async Task<IEnumerable<KeyValuePair<string, Document>>> GetByPrefix(string prefix, bool sortResults)
    {
        return await transactionProcessor.ProcessGetByPrefix(prefix, sortResults);
    }

    public async Task<TransactionResult> Remove(string key, int version)
    {
        var transaction = new Transaction();
        var documentChange = new DocumentChange();

        documentChange.AsDeleteChange(key, version);
        transaction.DocumentChange = documentChange;

        var result = await transactionProcessor.ProcessTransaction(transaction);

        return result;
    }
}