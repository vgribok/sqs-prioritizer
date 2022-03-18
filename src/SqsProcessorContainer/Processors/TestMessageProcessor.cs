using Amazon.SQS;
using Amazon.SQS.Model;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer
{
    /// <summary>
    /// Test processor (not for prod) simulating message processing failure
    /// </summary>
    public class TestMessageProcessor : TextMessageProcessor
    {
        private readonly OutputQueueSettings outputQueueSettings;

        private int skippedMessageCount = 0;

        public TestMessageProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
            this.outputQueueSettings = outputQueueSettings;
        }

        protected override Task ProcessPayload(
            string payload, 
            CancellationToken cancellationToken, 
            string receiptHandle, 
            int queueIndex, 
            string messageId,
            Dictionary<string, MessageAttributeValue> messageAttributes)
        {
            if(this.NeedToFailTheMessage)
                throw new Exception($"Intentionally failing message processing to return the message to the source queue (or to the DLQ)");

            this.logger.LogDebug("Successfully processed message via no-operation (drop)");

            return Task.CompletedTask;
        }

        protected override IEnumerable<string> ExpectMessageAttributes()
        {
            yield return PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName;
        }

        public bool NeedToFailTheMessage
        {
            get
            {
                if (this.outputQueueSettings.TestProcessOutputQueueFailEveryXMessage! == 0)
                    return false;

                if (++skippedMessageCount == this.outputQueueSettings.TestProcessOutputQueueFailEveryXMessage!)
                {
                    skippedMessageCount = 0;
                    return true;
                }

                return false;
            }
        }
    }
}
