#nullable enable

using Amazon;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using SqsDelay;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Almost "do nothing" SQS message processor example
    /// </summary>
    internal class NopMessageProcessor : SqsProcessor<MessageModel>
    {
        public NopMessageProcessor(Arn[] queueArns, 
                                    string processorId, 
                                    ILogger<NopMessageProcessor> logger, 
                                    int highPriorityWaitTimeoutSeconds, 
                                    int failureVisibilityTimeoutSeconds,
                                    int messageBatchSize) 
            : base(queueArns, processorId, logger, highPriorityWaitTimeoutSeconds, failureVisibilityTimeoutSeconds, messageBatchSize)
        {
        }

        /// <summary>
        /// An example of message payload processor
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="receiptHandle"></param>
        /// <param name="queueIndex"></param>
        /// <param name="messageId"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        protected override async Task ProcessPayload(MessageModel payload, string receiptHandle, int queueIndex, string messageId)
        {
            if (!string.IsNullOrWhiteSpace(payload.VisiblityTimeoutDuration))
                await UpdateMessageVisibilityTimeout(receiptHandle, queueIndex, Duration.ToTimeSpan(payload.VisiblityTimeoutDuration));

            if (payload.Throw ?? false)
                throw new Exception($"{Id(queueIndex)} message with Id {messageId} requested exception: \"{payload.Text}\"");

            logger.LogInformation($"{Id(queueIndex)} NOP-processed payload: \"{payload.Text}\"");
        }

        protected override Task HandlePayloadProcessingException(Exception ex, Message message, int queueIndex, TimeSpan failureVisibilityTimeout)
        {
            return base.HandlePayloadProcessingException(ex, message, queueIndex, failureVisibilityTimeout);
        }
    }
}
