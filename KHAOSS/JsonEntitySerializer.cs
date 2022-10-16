using System.Text.Json;

namespace KHAOSS;
public class JsonEntitySerializer : IEntitySerializer
{
    private readonly JsonSerializerOptions jsonSerializerOptions;

    public JsonEntitySerializer(JsonSerializerOptions jsonSerializerOptions)
    {
        if (jsonSerializerOptions == null)
        {
            jsonSerializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.General);
            jsonSerializerOptions.IgnoreReadOnlyFields = true;
            jsonSerializerOptions.WriteIndented = false;
        }
        this.jsonSerializerOptions = jsonSerializerOptions;
    }

    public T Deserialize<T>(byte[] data)
    {
        return JsonSerializer.Deserialize<T>(data);
    }

    public byte[] Serialize<T>(T entity)
    {
        return JsonSerializer.SerializeToUtf8Bytes(entity, jsonSerializerOptions);
    }
}