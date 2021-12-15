#nullable enable

// See https://aka.ms/new-console-template for more information

using SqsProcessorContainer;

public static class Program
{
    public static async Task Main()
    {   // TODO: do IoC

        using CancellationTokenSource cancellationSource = CreateAppExitCancellationSource();

        const int listenerCount = 3;
        List<NopMessageProcessor> listeners = SqsProcessor<MessageModel>.StartProcessors(
            listenerCount, (i) => new NopMessageProcessor(i)
        );

        IEnumerable<Task> listenerTasks = from l in listeners
                                          select l.Listen(cancellationSource.Token);
        
        await Task.WhenAll(listenerTasks);
    }

    private static CancellationTokenSource CreateAppExitCancellationSource()
    {   // TODO: Add support for SIGTERM or other ways of react to container termination
        var cancellationSource = new CancellationTokenSource();
        Console.CancelKeyPress += (object? sender, ConsoleCancelEventArgs e) =>
        {
            e.Cancel = true;
            cancellationSource.Cancel();
        };
        return cancellationSource;
    }
}
