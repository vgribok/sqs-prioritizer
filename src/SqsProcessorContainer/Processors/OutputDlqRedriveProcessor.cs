
using Amazon.SQS;
using Amazon.SQS.Model;
using aws_sdk_extensions;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace MessagePrioritizer.Processors
{
    /// <summary>
    /// Re-drives messages from output queue's DLQ into
    /// original input queues.
    /// </summary>
    internal class OutputDlqRedriveProcessor : TextMessageProcessor
    {
        private readonly OutputQueueSettings outputQueueSettings;

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

        public OutputDlqRedriveProcessor(
            SqsPrioritySettings settings,
            OutputQueueSettings outputQueueSettings,
            ILogger<TextMessageProcessor> logger, 
            IAmazonSQS sqsClient) 
            : base(
                    CreateOutputQueueSettings(outputQueueSettings.RedriveDlqArnString!, settings.ExpectedMessageAttributes), 
                    logger, 
                    sqsClient
              )
        {
            this.outputQueueSettings = outputQueueSettings;
        }

        protected override async Task ProcessPayload(
            string payload, 
            CancellationToken cancellationToken, 
            string receiptHandle, 
            int queueIndex, 
            string messageId, 
            Dictionary<string, MessageAttributeValue> messageAttributes)
        {
            string originalQueueUrl = messageAttributes[PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName].StringValue;
            var msgAttributes = messageAttributes.CloneMessageAttributes(PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName);

            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = originalQueueUrl,
                MessageBody = payload,
                MessageAttributes = msgAttributes,
                DelaySeconds = outputQueueSettings.DlqRedriveDelaySeconds
            };

            var response = await sqsClient.SendMessageAsync(sendMessageRequest, cancellationToken);

            this.logger.LogTrace("Re-drove DQL message to the original queue {OriginalQueue}, with new message id {OutputMessageId}.",
                                    originalQueueUrl,
                                    response.MessageId
            );
        }

        protected override IEnumerable<string> ExpectMessageAttributes()
        {   
            foreach(string atribute in base.ExpectMessageAttributes())
                yield return atribute;

            yield return PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName;
        }
    }
}
