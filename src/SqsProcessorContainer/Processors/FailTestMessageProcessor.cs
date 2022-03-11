using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer
{
    /// <summary>
    /// Test processor (not for prod) simulating message processing failure
    /// </summary>
    public class FailTestMessageProcessor : TextMessageProcessor
    {
        public FailTestMessageProcessor(
            SqsPrioritySettings settings, 
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
        }

        protected override Task ProcessPayload(
            string payload, 
            CancellationToken cancellationToken, 
            string receiptHandle, 
            int queueIndex, 
            string messageId,
            Dictionary<string, MessageAttributeValue> messageAttributes)
        {
            throw new Exception($"Intentionally failing message processing to return the message to the source queue (or to the DLQ)");
        }

        protected override IEnumerable<string> ExpectMessageAttributes()
        {
            yield return PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName;
        }
    }
}
