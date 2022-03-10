using Amazon.SQS;
using Amazon.SQS.Model;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer
{
    internal class PushToOutputQueueProcessor : TextMessageProcessor
    {
        const string sourceQueueUrlMessageAttributeName = "SourceQueue";

        private readonly OutputQueueSettings outputQueueSettings;

        public PushToOutputQueueProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(settings, logger, sqsClient)
        {
            this.outputQueueSettings = outputQueueSettings;
        }

        protected override async Task ProcessPayload(string payload, CancellationToken cancellationToken, 
            string receiptHandle, int queueIndex, string messageId)
        {
            var sourceQueueValue = new MessageAttributeValue
            {
                StringValue = this._queueUrls[queueIndex],
                DataType = "String"
            };

            var msgAttributes = new Dictionary<string, MessageAttributeValue>
            {
                [sourceQueueUrlMessageAttributeName] = sourceQueueValue
            };

            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = outputQueueSettings.OutputQueueUrl,
                MessageBody = payload,
                MessageAttributes = msgAttributes
            };

            var response = await sqsClient.SendMessageAsync(sendMessageRequest, cancellationToken);

            this.logger.LogTrace("Moved me message to the output queue, with new message id {OutputMessageId}.",
                                    response.MessageId);
        }
    }
}
