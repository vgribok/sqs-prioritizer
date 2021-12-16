#nullable enable

using Amazon;
using aws_sdk_extensions;
using SqsDelay;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Almost "do nothing" SQS message processor 
    /// </summary>
    internal class NopMessageProcessor : SqsProcessor<MessageModel>
    {
        public NopMessageProcessor(Arn queueArn, string processorId) 
            : base(queueArn, processorId)
        {
        }

        protected override async Task ProcessPayload(string receiptHandle, MessageModel payload)
        {
            if (!string.IsNullOrWhiteSpace(payload.VisiblityTimeoutDuration))
                await UpdateMessageVisibilityTimeout(receiptHandle, Duration.ToTimeSpan(payload.VisiblityTimeoutDuration));

            if (payload.Throw ?? false)
                throw new Exception($"Listener {_listenerId} message with receipt {SqsMessageExtensions.GetReceiptTail(receiptHandle)} requested exception: \"{payload.Text}\"");

            Console.WriteLine($"Listener {_listenerId} NOP-processed payload: \"{payload.Text}\"");
        }
    }
}
