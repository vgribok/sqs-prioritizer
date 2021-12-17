#nullable enable

// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using SqsProcessorContainer;
using SqsProcessorContainer.Models;

public class Program : BackgroundService
{
    public static IConfiguration? Configuration { get; private set; }

    public static Task Main(string[] args)
    {
        Configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            //.AddUserSecrets<Program>()
            .AddEnvironmentVariables()
            .AddCommandLine(args)
            .AddUserSecrets<Program>(optional: true)
            .Build();

        return CreateHostBuilder(args).Build().RunAsync();
    }

    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) => ConfigureServices(services));

    private static void ConfigureServices(IServiceCollection services)
    {
        services.AddHostedService<Program>();
        // Add other services below

        services.Configure<AppSettings>(Configuration!.GetSection("AppSettings"));
    }


    private AppSettings Settings { get; }

    public Program(IServiceProvider iocContainer, IOptionsSnapshot<AppSettings> settings)
    {
        Settings = settings.Value;
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
