using BenchmarkDotNet.Running;
using Sluice.Benchmarks;

if (args.Length > 0 && args[0] == "throughput")
{
    ThroughputHarness.Run();
    return;
}

if (args.Length > 0 && args[0] == "lifecycle")
{
    LifecycleHarness.Run();
    return;
}

BenchmarkSwitcher.FromAssembly(typeof(SerializationBenchmarks).Assembly).Run(args);
