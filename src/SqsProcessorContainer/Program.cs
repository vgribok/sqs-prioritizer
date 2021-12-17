#nullable enable

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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

    /// <summary>
    /// IoC-friendly constructor with parameters injected by DI container
    /// </summary>
    /// <param name="iocContainer"></param>
    /// <param name="settings">Injected class representing an app settings section.</param>
    public Program(IServiceProvider iocContainer, AppSettings settings)
    {
        Settings = settings;
    }
    
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        List<NopMessageProcessor> listeners = SqsProcessor<MessageModel>.StartProcessors(this.Settings.ProcessorCount,
            (i) => new NopMessageProcessor(this.Settings.QueueArnParsed, i.ToString())
        );

        IEnumerable<Task> listenerTasks = listeners.Select(l => l.Listen(stoppingToken));
        return Task.WhenAll(listenerTasks);
    }
}
