using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS.Benchmark
{



    [MemoryDiagnoser]
    public class KHAOSSBenchmarks
    {

        public int N = 999_999;
        private Connection<Entity> dataEngine;
        private string testKey = "123456";
        private Entity testDocument;

        public KHAOSSBenchmarks()
        {
            SetupDataEngine();
        }

        private void SetupDataEngine()
        {
            dataEngine = Connection<Entity>.CreateTransient(SourceGenerationContext.Default.Entity);
            dataEngine.OpenAsync(CancellationToken.None).Wait();

            Task[] setResults = new Task[N];
            for (int i = 0; i < N; i++)
            {
                setResults[i] = dataEngine.Store.Save(new Entity(i.ToString(), 0, false, $"Test Body {i}"));
            }
            Task.WaitAll(setResults);
            testDocument = dataEngine.Store.Get<Entity>(testKey);
        }

        [Benchmark]
        public void GetByKey()
        {
            var document = dataEngine.Store.Get<Entity>(testKey);
        }

        [Benchmark]
        public async Task SetKey()
        {
            testDocument = await dataEngine.Store.Save(testDocument);
        }

        [Benchmark]
        public void GetKeyPrefix()
        {
            var results = dataEngine.Store.GetByPrefix<Entity>("12345", false);
            foreach (var result in results)
            {
                if (result.Key == null)
                {
                    throw new Exception();
                }
            }
        }

    }
}
