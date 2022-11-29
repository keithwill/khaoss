using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KHAOSS.Test
{
    [JsonSourceGenerationOptions(WriteIndented = false)]
    [JsonSerializable(typeof(Entity))]
    [JsonSerializable(typeof(TestDocument))]
    internal partial class SourceGenerationContext : JsonSerializerContext
    {
    }

}
