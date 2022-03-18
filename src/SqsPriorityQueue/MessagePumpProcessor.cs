using Amazon.SQS;
using Amazon.SQS.Model;
using aws_sdk_extensions;
using Microsoft.Extensions.Logging;

namespace SqsPriorityQueue
{
    /// <summary>
    /// Implements message processor that moves messages from priority queues
    /// to another (output) queue.
    /// </summary>
    public abstract class MessagePumpProcessor : TextMessageProcessor
    {
        public MessagePumpProcessor(SqsPrioritySettings settings, ILogger<TextMessageProcessor> logger, IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
        }

        protected override async Task ProcessPayload(
            string payload, 
            CancellationToken cancellationToken, 
            string receiptHandle, 
            int queueIndex, 
            string messageId, 
            Dictionary<string, MessageAttributeValue> sourceMessageAttributes)
        {
            Dictionary<string, MessageAttributeValue>? outputMsgAttributes = this.AssembleOutboudMessageAttributes(sourceMessageAttributes, queueIndex);
            SendMessageRequest sendMessageRequest = CreateSendMessageRequest(payload, sourceMessageAttributes, outputMsgAttributes);

            var response = await sqsClient.SendMessageAsync(sendMessageRequest, cancellationToken);

            this.logger.LogDebug("Moved me message to the output queue {OutputQueueUrl}, with new message id {OutputMessageId}.",
                sendMessageRequest.QueueUrl,
                response.MessageId);
        }

        /// <summary>
        /// Creates basic send message request. Enables overriding in subclasses.
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="sourceMessageAttributes"></param>
        /// <returns></returns>
        protected virtual SendMessageRequest CreateSendMessageRequest(string payload, 
            Dictionary<string, MessageAttributeValue>? sourceMessageAttributes,
            Dictionary<string, MessageAttributeValue>? outputMessageAttributes
            )
            => new SendMessageRequest
                {
                    QueueUrl = this.GetOutputQueueUrl(sourceMessageAttributes),
                    MessageBody = payload,
                    MessageAttributes = outputMessageAttributes
            };

        /// <summary>
        /// Creates collection of the attributes for the outbound message.
        /// By default copies known attributes from the source message.
        /// </summary>
        /// <returns></returns>
        virtual protected Dictionary<string, MessageAttributeValue>? AssembleOutboudMessageAttributes(
            Dictionary<string, MessageAttributeValue> sourceMessageAttributes, 
            int queueIndex
        )
            => sourceMessageAttributes.CloneMessageAttributes();

        /// <summary>
        /// Supplies URL of the destination/output SQS queue
        /// </summary>
        /// <param name="messageAttributes"></param>
        /// <returns></returns>
        abstract protected string GetOutputQueueUrl(Dictionary<string, MessageAttributeValue>? messageAttributes);
    }
}
