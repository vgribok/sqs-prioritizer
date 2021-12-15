#nullable enable

using aws_sdk_extensions;
using SqsDelay;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Almost "do nothing" SQS message processor 
    /// </summary>
    internal class NopMessageProcessor : SqsProcessor<MessageModel>
    {
        public NopMessageProcessor(int listenerNumber) : base(listenerNumber)
        {
        }

        protected override async Task ProcessPayload(string receiptHandle, MessageModel payload)
        {
            if (!string.IsNullOrWhiteSpace(payload.VisiblityTimeoutDuration))
                await UpdateMessageVisibilityTimeout(receiptHandle, Duration.ToTimeSpan(payload.VisiblityTimeoutDuration));

            if (payload.Throw ?? false)
                throw new Exception($"Listener {_listenerNumber} message with receipt {SqsMessageExtensions.GetReceiptTail(receiptHandle)} requested exception: \"{payload.Text}\"");

            Console.WriteLine($"Listener {_listenerNumber} NOP-processed payload: \"{payload.Text}\"");
        }
    }
}
