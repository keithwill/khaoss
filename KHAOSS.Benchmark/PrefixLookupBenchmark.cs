using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KHAOSS.Benchmark
{
    [MemoryDiagnoser]
    //[SimpleJob(launchCount: 1, warmupCount: 1, targetCount: 5)]
    public class PrefixLookupBenchmark
    {

        private string[] testKeys;
        private string[] testBodyValues;
        private PrefixLookup<string> testPrefixLookup;
        private string prefixMatch = "0001";

        public int N = 10_000;

        public PrefixLookupBenchmark()
        {
            SetupTestData();
        }

        private void SetupTestData()
        {
            testKeys = new string[N];
            testBodyValues = new string[N];
            testPrefixLookup = new PrefixLookup<string>();

            string keyFormatString = "";
            int formatStringLength = N.ToString().Length;

            for(int i = 0; i < formatStringLength; i++)
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
            var result = testPrefixLookup.GetByPrefix(prefixMatch);
            foreach(var results in result)
            {
                if (results.Key == null)
                {
                    throw new Exception("Failed");
                }
            }
        }

        private bool first = true;

        [Benchmark]
        public void PrefixLookup_Get()
        {
            var result = testPrefixLookup.Get("01234");
        }

    }
}
