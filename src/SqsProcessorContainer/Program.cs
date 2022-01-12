#nullable enable

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqsPriorityQueue;
using SqsProcessorContainer;
using SqsProcessorContainer.Models;

internal class Program : BackgroundService
{
    public static Task Main(string[] args) => ConsoleApp.Init<Program>(args, ConfigureServices);

    private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.RegisterAppSettingsSection<AppSettings>(context.Configuration);

        services.RegisterProcessors<NopMessageProcessor>(isTheOnlyProcessorType: true, ProcessorCountFactory);
    }

    private static int ProcessorCountFactory(IServiceProvider ioc, Type processorType)
        => ioc.GetRequiredService<AppSettings>().ProcessorCount;

    private readonly List<NopMessageProcessor> processors;

    /// <summary>
    /// IoC-friendly constructor with parameters injected by DI container
    /// </summary>
    /// <param name="processors">Collection of processor instances</param>
    public Program(IEnumerable<NopMessageProcessor> processors)
    {
        this.processors = processors.ToList();
    }

    /// <summary>
    /// Main service/daemon execution loop
    /// </summary>
    /// <param name="stoppingToken">Ctrl-C and container SIGTERM source of stop signal</param>
    /// <returns></returns>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run processor listening loops
        IEnumerable<Task> processorTasks = processors.Select(p => p.Listen(stoppingToken));
        return Task.WhenAll(processorTasks);
    }
}
