using System.IO;
using System.Runtime.InteropServices;

namespace KHAOSS;

public class TransactionRecord
{
    public string Key;
    public int Version;
    public byte[] Body;
    public DocumentChangeType ChangeType;

    public int SizeInStore;

    private const int FIXED_HEADER_SIZE =
        4 + // Keylength
        4 + // Bodylength
        4 + // Version
        1;  // ChangeType

    public static TransactionRecord LoadFromStream(Stream stream, ref byte[] readBuffer)
    {
        Span<byte> headerBytes = stackalloc byte[FIXED_HEADER_SIZE];
        var lengthHeaderBytesRead = stream.Read(headerBytes);

        if (lengthHeaderBytesRead < FIXED_HEADER_SIZE)
        {
            return null;
        }

        var keyLength = BitConverter.ToInt32(headerBytes.Slice(0, 4));
        var bodyLength = BitConverter.ToInt32(headerBytes.Slice(4, 4));

        var version = BitConverter.ToInt32(headerBytes.Slice(8, 4));
        var changeType = (DocumentChangeType)headerBytes[12];

        var variableContentLength = keyLength + bodyLength;

        SizeBuffer(variableContentLength, ref readBuffer);
        var recordBytesRead = stream.Read(readBuffer, 0, variableContentLength);
        if (recordBytesRead != variableContentLength)
        {
            return null;
            //throw new Exception("Failed to read a record");
            //TODO: figure out how we want to log or recover from a record missing at the end of a file
        }
        var readSpan = readBuffer.AsSpan(0, recordBytesRead);

        var key = Encoding.UTF8.GetString(readSpan.Slice(0, keyLength));
        var body = readSpan.Slice(keyLength, bodyLength);

        var record = new TransactionRecord
        {
            Body = body.ToArray(),
            ChangeType = changeType,
            Key = key,
            Version = version,
            SizeInStore = FIXED_HEADER_SIZE + variableContentLength
        };
        return record;
    }

    private static void SizeBuffer(int recordLength, ref byte[] readBuffer)
    {
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
    }

    public void WriteTostream(Stream stream)
    {
        Span<byte> headerBytes = stackalloc byte[FIXED_HEADER_SIZE];

        var keyLength = Key.Length;
        var bodyLength = Body.Length;
        var version = Version;
        var changeType = (byte)ChangeType;

        MemoryMarshal.Write(headerBytes.Slice(0, 4), ref keyLength);
        MemoryMarshal.Write(headerBytes.Slice(4, 4), ref bodyLength);
        MemoryMarshal.Write(headerBytes.Slice(8, 4), ref version);
        MemoryMarshal.Write(headerBytes.Slice(12, 1), ref changeType);

        stream.Write(headerBytes);

        var keyBytes = Encoding.UTF8.GetBytes(Key);

        stream.Write(keyBytes);
        stream.Write(Body, 0, Body.Length);

        SizeInStore = FIXED_HEADER_SIZE + keyLength + bodyLength;
    }
}
