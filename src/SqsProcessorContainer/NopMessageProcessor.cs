#nullable enable

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
        public NopMessageProcessor(SqsPrioritySettings settings, ILogger<NopMessageProcessor> logger) 
            : base(settings, logger)
        {
        }

        /// <summary>
        /// An example of a message payload processor
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="receiptHandle"></param>
        /// <param name="queueIndex"></param>
        /// <param name="messageId"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        protected override async Task ProcessPayload(MessageModel payload, string receiptHandle, int queueIndex, string messageId)
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

            logger.LogInformation($"{Id(queueIndex)} NOP-processed payload: \"{payload.Text}\"");
        }

        protected override Task HandlePayloadProcessingException(Exception ex, Message message, int queueIndex, TimeSpan failureVisibilityTimeout)
        {
            return base.HandlePayloadProcessingException(ex, message, queueIndex, failureVisibilityTimeout);
        }
    }
}
