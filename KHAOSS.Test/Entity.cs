using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KHAOSS.Test
{
    [JsonDerivedType(typeof(TestDocument), typeDiscriminator: "TestDocument")]
    [JsonDerivedType(typeof(Entity), typeDiscriminator: "Entity")]
    public record class Entity(string Key, int Version, bool Deleted) : IEntity
    {
        public IEntity WithVersion(int version) { return this with { Version = version }; }
    }
}
