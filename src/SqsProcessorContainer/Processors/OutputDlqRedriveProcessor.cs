
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
    internal class OutputDlqRedriveProcessor : MessagePumpProcessor
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

        protected override SendMessageRequest CreateSendMessageRequest(string payload,
            Dictionary<string, MessageAttributeValue>? sourceMessageAttributes,
            Dictionary<string, MessageAttributeValue>? outputMessageAttributes
            )
        {
            SendMessageRequest sendMsgRequest = base.CreateSendMessageRequest(payload, sourceMessageAttributes, outputMessageAttributes);
            sendMsgRequest.DelaySeconds = outputQueueSettings.DlqRedriveDelaySeconds;
            return sendMsgRequest;
        }

        protected override IEnumerable<string> ExpectMessageAttributes()
        {   
            foreach(string atribute in base.ExpectMessageAttributes())
                yield return atribute;

            yield return PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName;
        }

        protected override string GetOutputQueueUrl(Dictionary<string, MessageAttributeValue>? messageAttributes)
            => messageAttributes![PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName].StringValue;

        protected override Dictionary<string, MessageAttributeValue>? AssembleOutboudMessageAttributes(
            Dictionary<string, MessageAttributeValue> messageAttributes, 
            int queueIndex
            )
            => messageAttributes.CloneMessageAttributes(PushToOutputQueueProcessor.sourceQueueUrlMessageAttributeName);
    }
}