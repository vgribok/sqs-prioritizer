#nullable enable
using CdkShared;
using Constructs;
using sqs_long_delay_construct;

namespace SqsLongDelay
{
    public class SqsLongDelayStack : BetterStack
    {
        private static string GetQueueName(Construct scope)
            => scope.GetCtxString("QueueName", "long-retry-test-queue");

        internal string QueueName => GetQueueName(scope: this);

        internal SqsLongDelayStack(Construct scope, string id = "SqsLongDelayStack", BetterStackProps? props = null) 
            : base(scope, id, InitStackProps(props))
        {
            new SqsWithLongDelayRetry(this, "The-Queue", new SqsWithLongDelayRetryProps
            {
                QueueName = this.QueueName
            });
        }

        private static BetterStackProps InitStackProps(BetterStackProps? props)
        {
            props ??= new BetterStackProps();

            props.DynamicStackNameGenerator ??= scope => $"Retry-SQS--{GetQueueName(scope)}";

            return props;
        }
    }
}
