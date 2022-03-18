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
    public class OutputQueueTestMessageProcessor : TestMessageProcessor
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

        public OutputQueueTestMessageProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient)
            : base(
                    CreateOutputQueueSettings(outputQueueSettings.OutputQueueArnString, settings.ExpectedMessageAttributes),
                    outputQueueSettings,
                    logger,
                    sqsClient
              )
        {
        }
    }
}
