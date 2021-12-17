#nullable enable

using Amazon;
using aws_sdk_extensions;
using Microsoft.Extensions.Logging;
using SqsDelay;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Almost "do nothing" SQS message processor 
    /// </summary>
    internal class NopMessageProcessor : SqsProcessor<MessageModel>
    {
        public NopMessageProcessor(Arn queueArn, string processorId, ILogger<NopMessageProcessor> logger) 
            : base(queueArn, processorId, logger)
        {
        }

        protected override async Task ProcessPayload(string receiptHandle, MessageModel payload)
        {
            if (!string.IsNullOrWhiteSpace(payload.VisiblityTimeoutDuration))
                await UpdateMessageVisibilityTimeout(receiptHandle, Duration.ToTimeSpan(payload.VisiblityTimeoutDuration));

            if (payload.Throw ?? false)
                throw new Exception($"Listener {_listenerId} message with receipt {SqsMessageExtensions.GetReceiptTail(receiptHandle)} requested exception: \"{payload.Text}\"");

            logger.LogInformation($"Listener {_listenerId} NOP-processed payload: \"{payload.Text}\"");
        }
    }
}
