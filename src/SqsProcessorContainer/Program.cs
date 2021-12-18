﻿#nullable enable

using Amazon;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqsProcessorContainer;
using SqsProcessorContainer.Models;

public class Program : BackgroundService
{
    public static Task Main(string[] args) => ConsoleApp.Init<Program>(args, ConfigureServices);

    private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        services.RegisterAppSettingsSection<AppSettings>(context.Configuration);
    }

    private AppSettings Settings { get; }

    private readonly IServiceProvider iocContainer;

    /// <summary>
    /// IoC-friendly constructor with parameters injected by DI container
    /// </summary>
    /// <param name="iocContainer"></param>
    /// <param name="settings">Injected class representing an app settings section.</param>
    public Program(IServiceProvider iocContainer, AppSettings settings)
    {
        this.iocContainer = iocContainer;
        this.Settings = settings;
    }
    
    /// <summary>
    /// Main service/daemon execution loop
    /// </summary>
    /// <param name="stoppingToken">Ctrl-C and container SIGTERM source of stop signal</param>
    /// <returns></returns>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var processorLogger = iocContainer.GetRequiredService<ILogger<NopMessageProcessor>>();

        Arn[] queueArns = this.Settings.QueueArnsParsed.ToArray();

        List<NopMessageProcessor> listeners = Enumerable.Range(1, this.Settings.ProcessorCount)
                    .Select(i => new NopMessageProcessor(
                        queueArns, 
                        i.ToString(), 
                        processorLogger, 
                        this.Settings.HighPriorityWaitTimeoutSeconds,
                        this.Settings.VisibilityTimeoutOnProcessingFailureSeconds)
                    ).ToList();

        IEnumerable<Task> listenerTasks = listeners.Select(l => l.Listen(stoppingToken));
        return Task.WhenAll(listenerTasks);
    }
}
