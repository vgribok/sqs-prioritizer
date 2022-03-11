using Amazon.SQS;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer.Processors
{
    public class OutputQueueFailTestMessageProcessor : FailTestMessageProcessor
    {
        protected static SqsPrioritySettings CreateOutputQueueSettings(string outputQueueArn) =>
            new SqsPrioritySettings
            {
                QueueArns = outputQueueArn,
                ProcessorCount = 1,
                MessageBatchSize = 10,
                HighPriorityWaitTimeoutSeconds = 20,
                VisibilityTimeoutOnProcessingFailureSeconds = 0
            };

        public OutputQueueFailTestMessageProcessor(
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(CreateOutputQueueSettings(outputQueueSettings.OutputQueueArnString), logger, sqsClient)
        {
        }
    }
}
