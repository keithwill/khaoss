using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS.Benchmark
{



    [MemoryDiagnoser]
    public class KHAOSSBenchmarks
    {

        private string[] testKeys;
        private string[] testBodyValues;
        private PrefixLookup<string> testPrefixLookup;
        private string prefixMatch = "001";

        public int N = 10_000;


        private DataEngine<Entity> dataEngine;
        private readonly Encoding utf8 = System.Text.Encoding.UTF8;
        private string testKey = "1234567890";
        private Entity testDocument;

        public KHAOSSBenchmarks()
        {
            SetupPrefixLookup();
            SetupDataEngine();
        }

        private void SetupDataEngine()
        {
            dataEngine = DataEngine<Entity>.CreateTransient(SourceGenerationContext.Default.Entity);
            dataEngine.StartAsync(CancellationToken.None).Wait();

            testDocument = new Entity(testKey, 0, false, "test document");
            testDocument = dataEngine.Store.Save(testDocument).Result;

            Task[] setResults = new Task[1000];
            for (int i = 0; i < 1000; i++)
            {
                setResults[i] = dataEngine.Store.Save(new Entity(i.ToString(), 0, false, $"Test Body {i}"));
            }
            Task.WaitAll(setResults);

        }

        private void SetupPrefixLookup()
        {
            testKeys = new string[N];
            testBodyValues = new string[N];
            testPrefixLookup = new PrefixLookup<string>();

            string keyFormatString = "";
            int formatStringLength = N.ToString().Length;

            for (int i = 0; i < formatStringLength; i++)
            {
                keyFormatString += "0";
            }

            for (int i = 0; i < N; i++)
            {
                testKeys[i] = i.ToString(keyFormatString);
                testBodyValues[i] = Guid.NewGuid().ToString();
            }

            for (int i = 0; i < N; i++)
            {
                testPrefixLookup.Add(testKeys[i], testBodyValues[i]);
            }
        }

        [Benchmark]
        public void PrefixLookup_PrefixMatch()
        {
            var result = testPrefixLookup.GetKeyValuePairByPrefix(prefixMatch);
            foreach (var results in result)
            {
                if (results.Key == null)
                {
                    throw new Exception("Failed");
                }
            }
        }

        [Benchmark]
        public void PrefixLookup_Get()
        {
            var result = testPrefixLookup.Get("01234");
        }

        [Benchmark]
        public async Task GetValueByKey()
        {
            var document = await dataEngine.Store.Get<Entity>(testKey);
        }

        [Benchmark]
        public async Task SetValueByKey()
        {
            testDocument = await dataEngine.Store.Save(testDocument);
        }


        [Benchmark]
        public async Task NoOp()
        {
            // Only useful for determining CPU usage issues during database idle loops
            await Task.Delay(100);
        }

        [Benchmark]
        public async Task GetValueByKeyPrefix()
        {
            var results = await dataEngine.Store.GetByPrefix<Entity>("123", false);
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
