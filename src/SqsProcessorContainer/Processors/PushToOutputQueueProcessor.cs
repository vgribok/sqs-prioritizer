using Amazon.SQS;
using Amazon.SQS.Model;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer
{
    /// <summary>
    /// Main prioritizer message pump moving
    /// messages from input queues to the output queue.
    /// </summary>
    internal class PushToOutputQueueProcessor : MessagePumpProcessor
    {
        public const string sourceQueueUrlMessageAttributeName = "SourceQueue";

        private readonly OutputQueueSettings outputQueueSettings;

        public PushToOutputQueueProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
            this.outputQueueSettings = outputQueueSettings;
        }

        protected override string GetOutputQueueUrl(Dictionary<string, MessageAttributeValue>? messageAttributes)
            => this.outputQueueSettings.OutputQueueUrl;

        protected override Dictionary<string, MessageAttributeValue>? AssembleOutboudMessageAttributes(
            Dictionary<string, MessageAttributeValue> sourceMessageAttributes, 
            int queueIndex)
        {
            var msgAttributes = base.AssembleOutboudMessageAttributes(sourceMessageAttributes, queueIndex);
            this.AddSourceQueueMessageAttribute(queueIndex, msgAttributes!);
            return msgAttributes;
        }

        /// <summary>
        /// Adds information about the source queue as a message attribute.
        /// This information is later used by the DLQ re-driver to re-drive
        /// failed messages back to the original SQS queue.
        /// </summary>
        /// <param name="queueIndex"></param>
        /// <param name="msgAttributes"></param>
        private void AddSourceQueueMessageAttribute(int queueIndex, Dictionary<string, MessageAttributeValue> msgAttributes)
        {
            var sourceQueueValue = new MessageAttributeValue
            {
                StringValue = this._queueUrls[queueIndex],
                DataType = "String"
            };
            msgAttributes![sourceQueueUrlMessageAttributeName] = sourceQueueValue;
        }
    }
}
