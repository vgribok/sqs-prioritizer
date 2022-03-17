using Amazon.SQS;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer.Processors
{
    /// <summary>
    /// Test (non-prod) processor failing processing of messages from
    /// output queue (moving them to DLQ).
    /// </summary>
    public class OutputQueueFailTestMessageProcessor : FailTestMessageProcessor
    {
        protected static SqsPrioritySettings CreateOutputQueueSettings(string outputDlqArn, string expectedMsgAttributes) =>
            new SqsPrioritySettings
            {
                QueueArns = outputDlqArn,
                ProcessorCount = 1,
                MessageBatchSize = 10,
                HighPriorityWaitTimeoutSeconds = 20,
                VisibilityTimeoutOnProcessingFailureSeconds = 0,
                ExpectedMessageAttributes = expectedMsgAttributes
            };

        public OutputQueueFailTestMessageProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient)
            : base(
                    CreateOutputQueueSettings(outputQueueSettings.OutputQueueArnString, settings.ExpectedMessageAttributes),
                    logger,
                    sqsClient
              )
        {
        }
    }
}
