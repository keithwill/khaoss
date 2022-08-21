using System.Diagnostics;
using System.IO;

namespace KHAOSS;

/// <summary>
/// A store that serializes transactions by appending them to the
/// end of a file and can only be be loaded by loading the entire file.
/// Shrinking removed data from the file doubles as backing up the transaction
/// store, as it requires rewriting.
/// </summary>
public class AppendOnlyStore : ITransactionStore, IDisposable
{
    private Stream outputStream;
    private readonly Func<Stream> rewriteStreamFactory;
    private readonly Func<Stream, Stream, Stream> swapRewriteStreamCallback;
    private readonly IMemoryStore memoryStore;

    private TransactionRecord serializationRecord;
    private Task flushTask;
    private int unflushed = 0;
    private object writeLock = new object();
    private Task rewriteTask;

    private MemoryStream rewriteTailBuffer;
    private Stream rewriteStream;

    public AppendOnlyStore(
        Stream outputStream,
        Func<Stream> rewriteStreamFactory,
        Func<Stream, Stream, Stream> swapRewriteStreamCallback,
        IMemoryStore memoryStore
        )
    {
        this.outputStream = outputStream;
        this.rewriteStreamFactory = rewriteStreamFactory;
        this.swapRewriteStreamCallback = swapRewriteStreamCallback;
        this.memoryStore = memoryStore;
        this.serializationRecord = new TransactionRecord();
    }

    private void Rewrite()
    {
        var startingLength = outputStream.Length;
        long endingLength = 0;
        //Console.WriteLine("Starting rewrite");
        var transactionRecord = new TransactionRecord();
        transactionRecord.ChangeType = DocumentChangeType.Set;

        var itemsRewritten = 0;


        foreach (var item in memoryStore.GetByPrefix("", false))
        {
            itemsRewritten++;
            transactionRecord.Key = item.Key;
            transactionRecord.Body = item.Value.Body;
            transactionRecord.Version = item.Value.Version;
            WriteRewriteTransaction(transactionRecord);
            if (rewriteTailBuffer.Length > 65536)
            {
                lock (writeLock)
                {
                    rewriteTailBuffer.Position = 0;
                    rewriteTailBuffer.CopyTo(rewriteStream);
                    rewriteTailBuffer.SetLength(0);
                }
            }
        }
        rewriteStream.Flush();

        var sw = Stopwatch.StartNew();

        lock (writeLock)
        {
            rewriteTailBuffer.WriteTo(rewriteStream);
            endingLength = rewriteStream.Length;
            outputStream = swapRewriteStreamCallback(outputStream, rewriteStream);
            rewriteStream.Dispose();
            rewriteTailBuffer.Dispose();
            rewriteStream = null;
            rewriteTailBuffer = null;
        }

        var elapsed = sw.Elapsed;
        //Console.WriteLine($"Rewrote db - {itemsRewritten} items in {elapsed.TotalMilliseconds} - FROM {startingLength} TO {endingLength}");

    }

    public void StartFileManagement()
    {

        if (flushTask == null)
        {
            flushTask = Task.Run(async () =>
           {
               while (true)
               {
                   await Task.Delay(1000);

                   if (unflushed > 0)
                   {
                       lock (writeLock)
                       {
                           if (unflushed == 0)
                           {
                               continue;
                           }

                           if (outputStream == null)
                           {
                               return;
                           }
                           if (outputStream is FileStream fileStream)
                           {
                               fileStream.Flush(true);
                           }
                           else
                           {
                               outputStream.Flush();
                           }

                           unflushed = 0;
                       }
                   }
               }

           });
        }

    }

    public IEnumerable<TransactionRecord> LoadRecords(CancellationToken cancellationToken)
    {
        // We only take an anemic approach to checking for load and write contention

        //var lengthBytes = new byte[4];
        var readBuffer = new byte[4096];

        while (!cancellationToken.IsCancellationRequested)
        {
            var transactionrecord = TransactionRecord.LoadFromStream(outputStream, ref readBuffer);
            if (transactionrecord == null)
            {
                yield break;
            }
            else
            {
                yield return transactionrecord;
            }
        }

    }

    public void Dispose()
    {
        if (outputStream == null)
        {
            return;
        }
        lock (writeLock)
        {
            if (unflushed > 0)
            {
                outputStream.Flush();
            }
            outputStream?.Dispose();
            outputStream = null;
        }
    }

    public void WriteTransaction(Transaction transaction)
    {
        if (transaction.IsSingleChange)
        {
            WriteDocumentChange(transaction.DocumentChange);
        }
        else
        {
            for (int i = 0; i < transaction.DocumentChanges.Length; i++)
            {
                WriteDocumentChange(transaction.DocumentChanges[i]);
            }
        }
        transaction.SetResult(TransactionResult.Complete);
    }

    private void WriteRewriteTransaction(TransactionRecord record)
    {
        record.WriteTostream(this.rewriteStream);
    }

    private void WriteDocumentChange(DocumentChange documentChange)
    {

        serializationRecord.Key = documentChange.Key;
        serializationRecord.ChangeType = documentChange.ChangeType;
        serializationRecord.Version = documentChange.Version;

        // serializationRecord.BodyHash TODO: calculate hash of body?

        if (documentChange.Document != null)
        {
            serializationRecord.Body = documentChange.Document.Body;
        }

        lock (writeLock)
        {
            serializationRecord.WriteTostream(outputStream);

            var written = serializationRecord.SizeInStore;
            unflushed += written;

            if (documentChange.ChangeType == DocumentChangeType.Set)
            {
                documentChange.Document.SizeInStore = written;
            }
            else if (documentChange.ChangeType == DocumentChangeType.Delete)
            {
                memoryStore.AddDeadSpace(written);
            }


            if (rewriteTailBuffer != null)
            {
                serializationRecord.WriteTostream(rewriteStream);
            }

            if (rewriteStream == null)
            {
                var ratio = memoryStore.DeadSpace / (double)outputStream.Length;
                if (ratio > .6 && memoryStore.DeadSpace > 10_000_000)
                {
                    rewriteTailBuffer = new MemoryStream();
                    rewriteStream = rewriteStreamFactory();

                    // These could be written to the tail buffer instead
                    // Putting them into the rewrite stream means they are out
                    // of order with when the transaction was done, but shouldn't matter
                    serializationRecord.WriteTostream(rewriteStream);

                    memoryStore.ResetDeadSpace();
                    rewriteTask = Task.Run(() => Rewrite());
                }
            }

        }

    }

    public void RemoveAllDocuments()
    {
        lock (writeLock)
        {
            outputStream.SetLength(0);
            outputStream.Flush();
            unflushed = 0;
        }
    }

    public Task ForceMaintenance()
    {
        lock (writeLock)
        {
            //no-op if we are already building a rewrite
            if (rewriteStream == null)
            {
                rewriteTailBuffer = new MemoryStream();
                rewriteStream = rewriteStreamFactory();
                memoryStore.ResetDeadSpace();
                rewriteTask = Task.Run(() => Rewrite());
            }
        }
        return rewriteTask;
    }
}