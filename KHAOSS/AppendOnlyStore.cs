using MessagePack;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace KHAOSS;

/// <summary>
/// A store that serializes transactions by appending them to the
/// end of a file and can only be be loaded by loading the entire file.
/// Shrinking removed data from the file doubles as backing up the transaction
/// store, as it requires rewriting.
/// </summary>
public class AppendOnlyStore : ITransactionStore, IDisposable
{

    private MessagePackSerializerOptions serializerOptions;
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
        //this.serializerOptions = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block);
        this.serializerOptions = MessagePackSerializerOptions.Standard;
        this.serializationRecord = new TransactionRecord();
    }

    private void Rewrite()
    {
        var startingLength = outputStream.Length;
        long endingLength = 0;
        Console.WriteLine("Starting rewrite");
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
        Console.WriteLine($"Rewrote db - {itemsRewritten} items in {elapsed.TotalMilliseconds} - FROM {startingLength} TO {endingLength}");

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
                           outputStream.Flush();
                           unflushed = 0;
                       }
                   }
               }

           });
        }

    }

    public async IAsyncEnumerable<TransactionRecord> LoadRecords([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // We only take an anemic approach to checking for load and write contention

        var lengthBytes = new byte[4];
        var readBuffer = new byte[4096];

        while (!cancellationToken.IsCancellationRequested)
        {
            var lengthBytesRead = await outputStream.ReadAsync(lengthBytes, 0, 4, cancellationToken);

            if (lengthBytesRead < 4)
            {
                break;
            }

            var recordLength = BitConverter.ToInt32(lengthBytes);

            // Make sure we have enough space in our read buffer
            // We don't reallocate a buffer for each read so that we can minimize allocation
            // Though byte array pooling could also be used here
            if (readBuffer.Length < recordLength)
            {
                // Attempt to minimize effect of slowly growing buffer size a bit
                var newBufferSize = readBuffer.Length;

                while (newBufferSize < recordLength)
                {
                    newBufferSize *= 2;
                }
                readBuffer = new byte[newBufferSize];
            }

            var recordBytesRead = await outputStream.ReadAsync(readBuffer, 0, recordLength, cancellationToken);

            if (recordBytesRead != recordLength)
            {
                throw new Exception("Failed to read a record");
                //TODO: figure out how we want to log or recover from a record missing at the end of a file
            }
            var recordMemory = readBuffer.AsMemory(0, recordLength);
            var record = MessagePackSerializer.Deserialize<TransactionRecord>(recordMemory, serializerOptions);
            record.SizeInStore = 4 + recordLength;

            yield return record;
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
        ReadOnlySpan<byte> recordBytes = MessagePackSerializer.Serialize(record, serializerOptions);
        Span<byte> lengthBytes = stackalloc byte[4];
        if (!BitConverter.TryWriteBytes(lengthBytes, recordBytes.Length))
        {
            throw new Exception("Could not estimate payload length");
        }
        ReadOnlySpan<byte> readOnlyLengthBytes = lengthBytes;
        var written = readOnlyLengthBytes.Length + recordBytes.Length;
        this.rewriteStream.Write(readOnlyLengthBytes);
        this.rewriteStream.Write(recordBytes);
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

        ReadOnlySpan<byte> recordBytes = MessagePackSerializer.Serialize(serializationRecord, serializerOptions);
        Span<byte> lengthBytes = stackalloc byte[4];
        if (!BitConverter.TryWriteBytes(lengthBytes, recordBytes.Length))
        {
            throw new Exception("Could not estimate payload length");
        }

        ReadOnlySpan<byte> readOnlyLengthBytes = lengthBytes;
        lock (writeLock)
        {
            var written = readOnlyLengthBytes.Length + recordBytes.Length;
            unflushed += written;

            if (documentChange.ChangeType == DocumentChangeType.Set)
            {
                documentChange.Document.SizeInStore = written;
            }
            else if (documentChange.ChangeType == DocumentChangeType.Delete)
            {
                memoryStore.AddDeadSpace(written);
            }

            outputStream.Write(readOnlyLengthBytes);
            outputStream.Write(recordBytes);

            if (rewriteTailBuffer != null)
            {
                rewriteTailBuffer.Write(readOnlyLengthBytes);
                rewriteTailBuffer.Write(recordBytes);
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
                    rewriteStream.Write(readOnlyLengthBytes);
                    rewriteStream.Write(recordBytes);

                    memoryStore.ResetDeadSpace();
                    rewriteTask = Task.Run(() => Rewrite());
                }
            }


            if (unflushed > 65536)
            {
                outputStream.Flush();
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