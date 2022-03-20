using System.Diagnostics;
using System.IO;

namespace KHAOSS;

public class DataEngine : IDataEngine, IDisposable
{
    private readonly ITransactionProcessor transactionProcessor;
    private readonly ITransactionStore transactionStore;
    private readonly IMemoryStore memoryStore;
    private readonly IDataStore store;

    public DataEngine(
        ITransactionProcessor transactionProcessor,
        ITransactionStore transactionStore,
        IMemoryStore memoryStore
        )
    {
        this.transactionProcessor = transactionProcessor;
        this.transactionStore = transactionStore;
        this.memoryStore = memoryStore;
        this.store = new DataStore(transactionProcessor);
    }

    public static DataEngine Create(string databaseFilePath)
    {
        var file = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        var memoryStore = new MemoryStore();
        var transactionStore = new AppendOnlyStore(
            file,
            () => new FileStream(Guid.NewGuid().ToString() + ".tmp", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None),
            (outputStream, rewriteStream) =>
            {
                var rewriteFileStream = (FileStream)rewriteStream;
                var rewriteFilePath = rewriteFileStream.Name;

                rewriteFileStream.Close();
                outputStream.Close();

                File.Replace(rewriteFilePath, databaseFilePath, null);

                var newOutputStream = new FileStream(databaseFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                newOutputStream.Position = newOutputStream.Length;
                newOutputStream.Flush();

                return newOutputStream;
            },
            memoryStore
            );
        var transactionProcessor = new TransactionQueueProcessor(transactionStore, memoryStore);
        return new DataEngine(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public static DataEngine CreateTransient()
    {
        var memoryStream = new MemoryStream();
        var memoryStore = new MemoryStore();
        var transactionStore = new AppendOnlyStore(
            memoryStream,
            () => new MemoryStream(),
            (outputStream, rewriteStream) =>
            {
                var newOutputStream = new MemoryStream();
                rewriteStream.CopyTo(newOutputStream);
                newOutputStream.Position = newOutputStream.Length;

                return newOutputStream;
            },
            memoryStore
        );
        var transactionProcessor = new TransactionQueueProcessor(transactionStore, memoryStore);
        return new DataEngine(
            transactionProcessor,
            transactionStore,
            memoryStore
        );
    }

    public IDataStore Store => store;

    public long DeadSpace => memoryStore.DeadSpace;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        this.transactionStore.StartFileManagement();
        await this.LoadRecordsFromStore(cancellationToken);
        await this.transactionProcessor.Start();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        this.disposing = true;
        await this.transactionProcessor.Stop();
        this.transactionStore.Dispose();
    }

    private async Task LoadRecordsFromStore(CancellationToken cancellationToken)
    {
        Stopwatch sw = new();
        sw.Start();
        await foreach (var record in this.transactionStore.LoadRecords(cancellationToken))
        {
            if (record.ChangeType == DocumentChangeType.Set)
            {
                var document = new Document
                {
                    Body = record.Body,
                    Version = record.Version,
                    SizeInStore = record.SizeInStore
                };

                memoryStore.LoadSet(record.Key, document);
            }
            else
            {
                memoryStore.LoadDelete(record.Key, record.Version, record.SizeInStore);
            }
        }
        sw.Stop();
        Console.WriteLine($"Finished loading from store: took {sw.Elapsed.TotalSeconds}");
    }

    private bool disposing = false;

    public void Dispose()
    {
        if (disposing)
        {
            return; // Already disposing
        }
        disposing = true;
        this.transactionProcessor.Stop().Wait();
        this.transactionStore.Dispose();
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
}