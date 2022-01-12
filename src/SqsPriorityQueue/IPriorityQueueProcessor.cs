using Microsoft.Extensions.DependencyInjection;

namespace SqsPriorityQueue
{
    public interface IPriorityQueueProcessor
    {
        public string? ListenerId { get; set; }

        public Task Listen(CancellationToken appExitRequestToken);

        
    }
}
