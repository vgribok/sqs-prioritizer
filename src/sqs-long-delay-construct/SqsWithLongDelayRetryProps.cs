#nullable enable

namespace sqs_long_delay_construct
{
    public class SqsWithLongDelayRetryProps
    {
        public string QueueName { get; set; } = "long-retry-queue";
    }
}
