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

        private bool isPaused;

        protected override bool IsPaused => this.isPaused;


        public PushToOutputQueueProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            OutputQueueDepthMonitor outputQueueDepthMonitor,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
            // Ensure queue depth monitor check ran an least once before message pump loop launches
            outputQueueDepthMonitor.CheckOutputQueueDepthAndNotifyPrioritizerMessagePump().Wait();

            this.outputQueueSettings = outputQueueSettings;
            outputQueueDepthMonitor.OnOutputQueuePaused += this.TogglePausedFlag;
            this.isPaused = outputQueueDepthMonitor.IsOutputQueuePaused;
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
                StringValue = this.queueUrls[queueIndex],
                DataType = "String"
            };
            msgAttributes![sourceQueueUrlMessageAttributeName] = sourceQueueValue;
        }

        private void TogglePausedFlag(bool needToPause)
        {
            if (this.isPaused == needToPause)
                return;

            this.isPaused = needToPause;
            this.logger.LogInformation("{FetchPauseAction} prioritizer message pump processor.", 
                            needToPause ? "Paused" : "Resumed");
        }
    }
}
