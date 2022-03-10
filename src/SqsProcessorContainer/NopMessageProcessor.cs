#nullable enable

using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using SqsDelay;
using SqsPriorityQueue;

namespace MessagePrioritizer
{
    /// <summary>
    /// Almost "do nothing" SQS message processor example.
    /// </summary>
    internal class NopMessageProcessor : SqsProcessor<MessageModel>
    {
        public NopMessageProcessor(SqsPrioritySettings settings, ILogger<NopMessageProcessor> logger, IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
        }

        /// <summary>
        /// An example of a message payload processor
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="receiptHandle"></param>
        /// <param name="queueIndex"></param>
        /// <param name="messageId"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        protected override async Task ProcessPayload(MessageModel payload, CancellationToken cancellationToken, string receiptHandle, int queueIndex, string messageId)
        {
            if (payload.Return ?? false)
            {
                int timeout = payload.Timeout == null ? 0 : (int)Duration.ToTimeSpan(payload.Timeout).TotalSeconds;

                payload.Return = false;
                payload.Timeout = null;
                payload.Text += " Reprocessed!";

                await this.ReturnMessageToQueue(payload, queueIndex, timeout);
            }else
            {
                if (!string.IsNullOrWhiteSpace(payload.Timeout))
                    await UpdateMessageVisibilityTimeout(receiptHandle, queueIndex, Duration.ToTimeSpan(payload.Timeout));

                if (payload.Throw ?? false)
                    throw new Exception($"{Id(queueIndex)} message with Id {messageId} requested exception: \"{payload.Text}\"");
            }

            logger.LogInformation("Completed No-Op payload processing");
        }

        protected override Task HandlePayloadProcessingException(Exception ex, Message message, int queueIndex, TimeSpan failureVisibilityTimeout)
        {
            return base.HandlePayloadProcessingException(ex, message, queueIndex, failureVisibilityTimeout);
        }
    }
}
