#nullable enable
using CdkShared;
using Constructs;

namespace SqsLongDelay
{
    public class SqsLongDelayStack : BetterStack
    {
        private static string GetQueueName(Construct scope)
            => scope.GetCtxString("QueueName", "long-retry-test-queue");

        internal SqsLongDelayStack(Construct scope, string id = "SqsLongDelayStack", BetterStackProps? props = null) 
            : base(scope, id, InitStackProps(props))
        {
            // The code that defines your stack goes here
        }

        private static BetterStackProps InitStackProps(BetterStackProps? props)
        {
            props ??= new BetterStackProps();

            props.DynamicStackNameGenerator ??= scope => $"Retry-SQS--{GetQueueName(scope)}";

            return props;
        }
    }
}
