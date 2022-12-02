using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using KHAOSS.Benchmark;
//using KHAOSS.Benchmark;

public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<KHAOSSBenchmarks>(config: DefaultConfig.Instance.AddJob(Job.Default.WithMaxRelativeError(.05)));
        Console.WriteLine("Press enter to continue to ConcurrentDictionaryTests");
        Console.ReadLine();
        var summary2 = BenchmarkRunner.Run<ConcurrentDictionaryBenchmarks>(config: DefaultConfig.Instance.AddJob(Job.Default.WithMaxRelativeError(.05)));
    }
}