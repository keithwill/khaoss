using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace KHAOSS;

/// <summary>
/// A store that serializes transactions by appending them to the
/// end of a file and can only be be loaded by loading the entire file.
/// Shrinking removed data from the file doubles as backing up the transaction
/// store, as it requires rewriting.
/// </summary>
public class AppendOnlyStore<T> : IDisposable where T : class, IEntity
{
    private Stream outputStream;
    private readonly Func<Stream> rewriteStreamFactory;
    private readonly Func<Stream, Stream, Stream> swapRewriteStreamCallback;
    private readonly MemoryStore<T> memoryStore;
    private readonly JsonTypeInfo<T> jsonTypeInfo;
    private Task flushTask;
    private int unflushed = 0;
    private object writeLock = new object();
    private Task rewriteTask;
    private bool isDisposing = false;

    private MemoryStream rewriteTailBuffer;
    private Stream rewriteStream;

    public AppendOnlyStore(
        Stream outputStream,
        Func<Stream> rewriteStreamFactory,
        Func<Stream, Stream, Stream> swapRewriteStreamCallback,
        MemoryStore<T> memoryStore,
        JsonTypeInfo<T> jsonTypeInfo
        )
    {
        this.outputStream = outputStream;
        this.rewriteStreamFactory = rewriteStreamFactory;
        this.swapRewriteStreamCallback = swapRewriteStreamCallback;
        this.memoryStore = memoryStore;
        this.jsonTypeInfo = jsonTypeInfo;
    }

    private void Rewrite()
    {
        var startingLength = outputStream.Length;
        long endingLength = 0;
        //Console.WriteLine("Starting rewrite");

        var itemsRewritten = 0;

        foreach (var item in memoryStore.GetByPrefix("", false))
        {
            if (isDisposing)
            {
                if (rewriteStream is FileStream fs)
                {
                    string path = fs.Name;
                    fs.Dispose();
                    File.Delete(path);
                    //Console.WriteLine("Cancelled rewrite");
                }
                return;
            }
            if (itemsRewritten > 0)
            {
                this.rewriteStream.WriteByte(LF);
            }
            JsonSerializer.Serialize(rewriteStream, item, jsonTypeInfo);
            if (rewriteTailBuffer.Length > 65536)
            {
                lock (writeLock)
                {
                    rewriteTailBuffer.Position = 0;
                    rewriteTailBuffer.CopyTo(rewriteStream);
                    rewriteTailBuffer.SetLength(0);
                }
            }
            itemsRewritten++;
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
               while (!isDisposing)
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

    public IEnumerable<T> LoadRecords(CancellationToken cancellationToken)
    {

        using var sr = new StreamReader(outputStream, System.Text.Encoding.UTF8, leaveOpen: true);

        while (!cancellationToken.IsCancellationRequested)
        {
            var line = sr.ReadLine();
            if (line == null)
            {
                yield break;
            }
            if (line.Length > 0)
            {
                var entity = JsonSerializer.Deserialize<T>(line, jsonTypeInfo);
                yield return entity;
            }
        }

    }

    public void Dispose()
    {
        isDisposing = true;
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

    public void WriteTransaction(Transaction<T> transaction)
    {
        if (transaction.IsSingleChange)
        {
            WriteDocumentChange(transaction.Entity);
        }
        else
        {
            for (int i = 0; i < transaction.Entities.Length; i++)
            {
                WriteDocumentChange(transaction.Entities[i]);
            }
        }
        transaction.Complete();
    }

    private const byte LF = 10;

    private void WriteDocumentChange(T entity)
    {
        lock (writeLock)
        {
            var startingPosition = outputStream.Position;
            bool writeLF = false;
            if (startingPosition > 0)
            {
                writeLF = true;
                outputStream.WriteByte(LF);
            }
            JsonSerializer.Serialize(outputStream, entity, jsonTypeInfo);
            var written = outputStream.Position - startingPosition;
            unflushed += (int)written;

            if (rewriteTailBuffer != null)
            {
                if (writeLF) rewriteStream.WriteByte(LF);
                JsonSerializer.Serialize(rewriteStream, entity, jsonTypeInfo);
            }

            if (rewriteStream == null)
            {
            
                var ratio = memoryStore.DeadEntityCount / memoryStore.EntityCount;
                if (ratio > 1.0 && memoryStore.EntityCount > 10000)
                {
                    rewriteTailBuffer = new MemoryStream();
                    rewriteStream = rewriteStreamFactory();

                    // These could be written to the tail buffer instead
                    // Putting them into the rewrite stream means they are out
                    // of order with when the transaction was done, but shouldn't matter
                    if (writeLF) rewriteStream.WriteByte(LF);
                    JsonSerializer.Serialize(rewriteStream, entity, jsonTypeInfo);

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

    public Task RewriteTask => rewriteTask;
}