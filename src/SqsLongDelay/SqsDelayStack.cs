#nullable enable
using CdkShared;
using Constructs;
using sqs_delay_construct;
using System;

namespace SqsDelay
{
    public class SqsDelayStack : BetterStack
    {
        private static string GetQueueName(Construct scope)
            => scope.GetCtxString("QueueName", "retry-test-queue");

        internal string QueueName => GetQueueName(scope: this);
        internal int DlqMaxReceiveCount => int.Parse(this.GetCtxString("DlqMaxReceiveCount", "2"));

        internal TimeSpan VisibilityTimeout =>
            Duration.ToTimeSpan(this.GetCtxString("VisibilityTimeout", "1m"));

        internal TimeSpan Delay =>
            Duration.ToTimeSpan(this.GetCtxString("Delay", "0s"));

        internal TimeSpan DlqVisibilityTimeout =>
            Duration.ToTimeSpan(this.GetCtxString("DlqVisibilityTimeout", "10s"));

        internal TimeSpan DlqDelay =>
            Duration.ToTimeSpan(this.GetCtxString("DlqDelay", "45s"));

        private static BetterStackProps InitStackProps(BetterStackProps? props)
        {
            props ??= new BetterStackProps();

            props.DynamicStackNameGenerator ??= scope => $"Retry-SQS--{GetQueueName(scope)}";

            return props;
        }

        internal SqsDelayStack(Construct scope, string id = "SqsDelayStack", BetterStackProps? props = null) 
            : base(scope, id, InitStackProps(props))
        {
            new SqsWithDelayRetry(this, "The-Queue", new SqsWithDelayRetryProps
            {
                QueueName = this.QueueName,
                VisibilityTimeout = this.VisibilityTimeout,
                Delay = this.Delay,
                DlqMaxReceiveCount = this.DlqMaxReceiveCount,
                DlqVisibilityTimeout = this.DlqVisibilityTimeout,
                DlqDelay = this.DlqDelay,
            });
        }
    }
}
