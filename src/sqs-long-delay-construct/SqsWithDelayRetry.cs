#nullable enable
using Constructs;
using Amazon.CDK.AWS.SQS;
using Amazon.CDK;

namespace sqs_delay_construct
{
    /// <summary>
    /// Provisions an SQS queue/DLQ pair, plus other infra components for enabling 
    /// long delays between message processing retry attempts.
    /// </summary>
    public class SqsWithDelayRetry : Construct
    {
        public SqsWithDelayRetry(Construct scope, string id, SqsWithDelayRetryProps props)
            : base(scope, id)
        {
            //var dlq = new DeadLetterQueue(this, "DLQ", new DeadLetterQueueProps)
            var dlq = new Queue(this, "DLQ", new QueueProps
            {
                QueueName = $"{props.QueueName}-DLQ",
                DeliveryDelay = Duration.Millis(props.DlqDelay.TotalMilliseconds),
                VisibilityTimeout = Duration.Millis(props.DlqVisibilityTimeout.TotalMilliseconds),
            });

            var q = new Queue(this, "Main Queue", new QueueProps
            {
                QueueName = props.QueueName,
                DeadLetterQueue = new DeadLetterQueue { Queue = dlq, MaxReceiveCount = props.DlqMaxReceiveCount },
                DeliveryDelay = Duration.Millis(props.Delay.TotalMilliseconds),
                VisibilityTimeout = Duration.Millis(props.VisibilityTimeout.TotalMilliseconds),
            });
        }
    }
}
