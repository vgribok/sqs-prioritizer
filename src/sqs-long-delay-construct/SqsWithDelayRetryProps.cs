#nullable enable

namespace sqs_delay_construct
{
    public class SqsWithDelayRetryProps
    {
        public string QueueName { get; set; } = "long-retry-queue";
        public int DlqMaxReceiveCount { get; set; } = 0;
        public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan Delay { get; set; } = TimeSpan.Zero;
        public TimeSpan DlqVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan DlqDelay { get; set; } = TimeSpan.FromSeconds(45);
    }
}
