#nullable enable

using Amazon;
using aws_sdk_extensions;

namespace MessagePrioritizer.Models
{
    /// <summary>
    /// Configuration settings for the queue with 
    /// prioritized messages
    /// </summary>
    public class OutputQueueSettings
    {
        /// <summary>
        /// ARN of the SQS queue where prioritized messages are outputted.
        /// Customer processors pick messages from this queue.
        /// </summary>
        /// <remarks>
        /// Prioritizer message pump picks messages from multiple priority 
        /// queues and pushes messages in the output queue to simplify
        /// customer processor logic. All that customer processor need to
        /// care about is to process messages out of this queue quickly.
        /// </remarks>
        public string OutputQueueArnString { get; set; } = string.Empty;

        /// <summary>
        /// The max number of items to be placed in the output queue
        /// before prioritized message pump is throttled.
        /// </summary>
        /// <remarks>
        /// Output queue may have a mix of items of different priorities.
        /// Growing this queue too much will result in lower-priority items
        /// getting in the way of high priority items. The prioritizer pump 
        /// processor, therefore, will pause to let the customer processor 
        /// catch up if this queue depth is exceeded.
        /// </remarks>
        public int ThrottleQueueDepth { get; set; }

        /// <summary>
        /// ARN of the DLQ from which messages would be re-driven into the
        /// original input (priority) queues
        /// </summary>
        public string? RedriveDlqArnString { get; set; }

        /// <summary>
        /// Specifies how often message pump processor checks output queue
        /// </summary>
        public int QueueDepthCheckFrequencySeconds { get; set; }

        /// <summary>
        /// Static (not exp-back-off) visibility delay for each message
        /// re-driven from output DLQ
        /// </summary>
        public int DlqRedriveDelaySeconds { get; set; } = 0;

        /// <summary>
        /// If not null, a test processor of the output queue will be
        /// attached to the output queue. It will fail every X message
        /// specified by this property.
        /// </summary>
        /// <remarks>
        /// Set to null to disable test processor of the output queue.
        /// Set to 0 to successfully process (log) all messages.
        /// Set to 1 to fail all messages.
        /// Set to 4 to fail 1/4 (25%) of all messages.
        /// </remarks>
        public int? TestProcessOutputQueueFailEveryXMessage { get; set; } = null;

        public Arn OutputQueueArn => Arn.Parse(this.OutputQueueArnString);

        public string OutputQueueUrl => this.OutputQueueArn.SqsArnToUrl();
    }
}
