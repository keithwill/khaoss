using KHAOSS;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace KHAOSS.ConsoleTest
{
    class Program
    {
        static DataEngine engine;
       

        static async Task Main(string[] args)
        {
            if (System.IO.File.Exists("test.db"))
            {
                System.IO.File.Delete("test.db");
            }

            engine = DataEngine.Create("test.db");
            await engine.StartAsync(CancellationToken.None);

            for (int i = 1; i < 1_000; i++)
            {
                await SetValue("garbageKey" + i.ToString(), "This is some garbagasas dfe,This is some garbage,Thisasd fasd fas is some garbage,d asdf asThis is some garbage1", 1);
                await SetValue("garbageKey" + i.ToString(), "This is some garbagasas dfe,This is some garbage,Thisasd fasd fas is some garbage,d asdf asThis is some garbage2", 2);
                await SetValue("garbageKey" + i.ToString(), "This is some garbagasas dfe,This is some garbage,Thisasd fasd fas is some garbage,d asdf asThis is some garbage2", 3);
                await SetValue("garbageKey" + i.ToString(), "This is some garbagasas dfe,This is some garbage,Thisasd fasd fas is some garbage,d asdf asThis is some garbage2", 4);
            }

            
            var allDocs2 = await engine.Store.GetByPrefix("", false);
            var allDocsCount2 = allDocs2.Count();
            Console.WriteLine("ALL Docs Count: " + allDocsCount2);


            await engine.ForceMaintenance();
            engine.Dispose();

            engine = DataEngine.Create("test.db");
            await engine.StartAsync(CancellationToken.None);

            var allDocs3 = await engine.Store.GetByPrefix("", false);
            var allDocsCount3 = allDocs3.Count();
            Console.WriteLine("ALL Docs After Maintenance Count: " + allDocsCount2);
            Console.ReadKey();
            return;
        }

        private static async Task TimeIterations(string activityName, Func<int, int, Task> toDo, int threads, int iterations)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var tasks = new Task[threads];
            var threadLatencyAverages = new double[threads];
            var threadLatencyMaxes = new double[threads];
            var threadLatencyMedian = new double[threads];

            for(int threadI = 0; threadI < threads; threadI++)
            {
                var threadCount = threadI;
                tasks[threadI] = Task.Run(async () =>
                {
                    var taskLatencies = new double[iterations];
                    Stopwatch sw = new Stopwatch();
                    for (int iterationCount = 0; iterationCount < iterations; iterationCount++)
                    {
                        sw.Start();
                        await toDo(threadCount, iterationCount);
                        sw.Stop();
                        taskLatencies[iterationCount] = sw.Elapsed.TotalMilliseconds;
                        sw.Reset();
                    }
                    threadLatencyAverages[threadCount] = taskLatencies.Average();
                    threadLatencyMaxes[threadCount] = taskLatencies.Max();
                    var halfIndex = iterations / 2;
                    threadLatencyMedian[threadCount] = taskLatencies[halfIndex];
                });
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();
            var elapsed = stopwatch.Elapsed;

            var totalIterations = threads * iterations;
            var throughput = totalIterations / elapsed.TotalSeconds;

            var threadMedianSorted = threadLatencyMedian.AsEnumerable().OrderBy(x => x).ToList();

            var overallMedian = threadMedianSorted[threadMedianSorted.Count / 2];

            Console.WriteLine($"{activityName} Total Elapsed: {elapsed} - Throughput:{throughput}/s");
            Console.WriteLine($"{activityName} Average Latency by Thread - Avg:{threadLatencyAverages.Average()} Median:{overallMedian} Max:{threadLatencyAverages.Max()}");
        }

        private static async Task SetValue(string key, string body, int version)
        {
            var bodyBytes = System.Text.Encoding.UTF8.GetBytes(body);
            var result = await engine.Store.Set(key, new Document { Version = version, Body = bodyBytes });
        }

        private static async Task KHAOSSEngineSetKeyTransaction(int threadCount, int threadIteration)
        {
            var key = "/test/" + threadCount.ToString() + "/" + threadIteration.ToString();
            var body = $"Hello World from {key}";

//             var body = @"In computer science, a radix tree (also radix trie or compact prefix tree) is a data structure that represents a space-optimized trie (prefix tree) in which each node that is the only child is merged with its parent. The result is that the number of children of every internal node is at most the radix r of the radix tree, where r is a positive integer and a power x of 2, having x ≥ 1. Unlike regular trees, edges can be labeled with sequences of elements as well as single elements. This makes radix trees much more efficient for small sets (especially if the strings are long) and for sets of strings that share long prefixes.
// Unlike regular trees (where whole keys are compared en masse from their beginning up to the point of inequality), the key at each node is compared chunk-of-bits by chunk-of-bits, where the quantity of bits in that chunk at that node is the radix r of the radix trie. When the r is 2, the radix trie is binary (i.e., compare that node's 1-bit portion of the key), which minimizes sparseness at the expense of maximizing trie depth—i.e., maximizing up to conflation of nondiverging bit-strings in the key. When r is an integer power of 2 having r ≥ 4, then the radix trie is an r-ary trie, which lessens the depth of the radix trie at the expense of potential sparseness.
// As an optimization, edge labels can be stored in constant size by using two pointers to a string (for the first and last elements).[1]
// Note that although the examples in this article show strings as sequences of characters, the type of the string elements can be chosen arbitrarily; for example, as a bit or byte of the string representation when using multibyte character encodings or Unicode.";

            var bodyBytes = System.Text.Encoding.UTF8.GetBytes(body);
            var result = await engine.Store.Set(key, new Document { Version = 1, Body = bodyBytes });
        }

        private static async Task KHAOSSEngineGetKeyTransaction(int threadCount, int threadIteration)
        {
            var key = $"/test/{threadCount}/{threadIteration}";
            var result = await engine.Store.Get(key);
        }
    }
}
