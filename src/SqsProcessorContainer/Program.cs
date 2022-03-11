﻿#nullable enable

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

        //services.RegisterProcessors<NopMessageProcessor>(RetrieveProcessorCountSetting, isTheOnlyProcessorType: true);
        services.RegisterProcessors<PushToOutputQueueProcessor>(RetrieveProcessorCountSetting);
        // Uncomment the following line to test fail-processing messages in the output queue. (This is for testing purposes only)
        //services.RegisterProcessors<OutputQueueFailTestMessageProcessor>((ioc, ptype) => 1);
    }

    private static int RetrieveProcessorCountSetting(IServiceProvider ioc, Type processorType)
        => ioc.GetRequiredService<SqsPrioritySettings>().ProcessorCount;

    private readonly List<IPriorityQueueProcessor> processors;

    /// <summary>
    /// IoC-friendly constructor with parameters injected by DI container
    /// </summary>
    /// <param name="processors">Collection of processor instances</param>
    public Program(
        IEnumerable<PushToOutputQueueProcessor> pumpProcessors
        // Uncomment the following line to test fail-processing messages in the output queue. (This is for testing purposes only)
        //IEnumerable<OutputQueueFailTestMessageProcessor> outputFailProcessors
        )
    {
        this.processors =
            pumpProcessors.Cast<IPriorityQueueProcessor>()
            // Uncomment the following line to test fail-processing messages in the output queue. (This is for testing purposes only)
            //.Concat(outputFailProcessors.Cast<IPriorityQueueProcessor>())
            .ToList();
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
