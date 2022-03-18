#nullable enable

using Amazon.Extensions.NETCore.Setup;
using Amazon.SQS;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqsPriorityQueue;
using MessagePrioritizer;
using MessagePrioritizer.Models;
using MessagePrioritizer.Processors;

internal class Program : BackgroundService
{
    /// <summary>
    /// Application entry point
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    public static Task Main(string[] args) => ConsoleApp.Init<Program>(args, ConfigureServices);

    /// <summary>
    /// The place to configure DI container
    /// </summary>
    /// <param name="context"></param>
    /// <param name="services"></param>
    private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.RegisterAppSettingsSection<SqsPrioritySettings>(context.Configuration);
        services.RegisterAppSettingsSection<OutputQueueSettings>(context.Configuration);

        // Register Amazon services
        AWSOptions? awsSettings = context.Configuration.GetAWSOptions();
        services.AddDefaultAWSOptions(awsSettings);
        services.AddAWSService<IAmazonSQS>();

        //services.RegisterProcessors<NopMessageProcessor>(RetrieveProcessorCountSetting, isTheOnlyProcessorType: false);
        services.RegisterProcessors<PushToOutputQueueProcessor>(RetrieveProcessorCountSetting);

        OutputQueueSettings outputQueueSettings = context.Configuration
                                            .GetSection(nameof(OutputQueueSettings))
                                            .Get<OutputQueueSettings>();

        if(outputQueueSettings.TestProcessOutputQueueFailEveryXMessage != null)
            services.RegisterProcessors<OutputQueueTestMessageProcessor>((ioc, ptype) => 1);

        if (!string.IsNullOrWhiteSpace(outputQueueSettings.RedriveDlqArnString))
            services.RegisterProcessors<OutputDlqRedriveProcessor>((ioc, ptype) => 1);
    }

    private static int RetrieveProcessorCountSetting(IServiceProvider ioc, Type processorType)
        => ioc.GetRequiredService<SqsPrioritySettings>().ProcessorCount;

    private readonly List<IPriorityQueueProcessor> processors;

    /// <summary>
    /// IoC-friendly constructor with parameters injected by DI container
    /// </summary>
    /// <param name="processors">Collection of processor instances</param>
    public Program(
        IEnumerable<PushToOutputQueueProcessor> pumpProcessors,
        IEnumerable<OutputDlqRedriveProcessor> dlqRedrivers,
        IEnumerable<OutputQueueTestMessageProcessor> outputFailProcessors,
        IEnumerable<NopMessageProcessor> nopMessageProcessors
        )
    {
        this.processors = MergeProcessors(pumpProcessors, dlqRedrivers, outputFailProcessors, nopMessageProcessors)
                            .ToList();
    }

    private static IEnumerable<IPriorityQueueProcessor> MergeProcessors(params IEnumerable<object>?[] processors)
        => processors.Where(processorSet => processorSet != null)
                    .SelectMany(processorSet2 => processorSet2!.Cast<IPriorityQueueProcessor>());

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
