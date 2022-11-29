using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS.Test
{
    public record TestDocument(string Key, int Version, bool Deleted, string Body) : Entity(Key, Version, Deleted);

}
