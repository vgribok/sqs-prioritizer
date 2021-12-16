#nullable enable

// See https://aka.ms/new-console-template for more information

using Amazon;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SqsProcessorContainer;

public class Program : BackgroundService
{
    public static Task Main(string[] args) =>
        CreateHostBuilder(args).Build().RunAsync();

    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
                services.AddHostedService<Program>());

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        const int listenerCount = 3;
        Arn queueArn = Arn.Parse("arn:aws:sqs:us-east-2:444039259723:delme");

        List<NopMessageProcessor> listeners = SqsProcessor<MessageModel>.StartProcessors(listenerCount,
            (i) => new NopMessageProcessor(queueArn, i.ToString())
        );

        IEnumerable<Task> listenerTasks = listeners.Select(l => l.Listen(stoppingToken));

        return Task.WhenAll(listenerTasks);
    }
}
