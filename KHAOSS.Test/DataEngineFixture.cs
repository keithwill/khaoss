using KHAOSS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS.Test
{
    // https://xunit.net/docs/shared-context
    public class DataEngineFixture : IDisposable
    {
        public Connection<Entity> Engine { get; private set; }
        public Store<Entity> Store => Engine.Store;

        public DataEngineFixture()
        {
            this.Engine = Connection<Entity>.CreateTransient(SourceGenerationContext.Default.Entity);
            this.Engine.OpenAsync(CancellationToken.None).Wait();
        }

        public void Dispose()
        {
            this.Engine.Close(CancellationToken.None).Wait();

        }
    }
}
