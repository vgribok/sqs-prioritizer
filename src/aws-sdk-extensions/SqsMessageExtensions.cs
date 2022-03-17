#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using static Amazon.Lambda.SQSEvents.SQSEvent;

namespace aws_sdk_extensions
{
    public static class SqsMessageExtensions
    {
        public static Task SetVisibilityTimeout(this SQSMessage qmsg, TimeSpan delay) =>
            SetVisibilityTimeout(Arn.Parse(qmsg.EventSourceArn), qmsg.ReceiptHandle, delay);

        public static async Task SetVisibilityTimeout(Arn queueArn, string messageReceiptHandle, TimeSpan delay)
        {
            using var client = new AmazonSQSClient(RegionEndpoint.GetBySystemName(queueArn.Region));
            await client.SetVisibilityTimeout(queueArn, messageReceiptHandle, delay);
        }

        public static Task SetVisibilityTimeout(this IAmazonSQS client, Arn queueArn, string messageReceiptHandle, TimeSpan delay)
        {
            var changeMsgVisReq = new ChangeMessageVisibilityRequest
            {
                QueueUrl = MiscExtensions.SqsArnToUrl(queueArn),
                ReceiptHandle = messageReceiptHandle,
                VisibilityTimeout = (int)delay.TotalSeconds
            };

            return client.ChangeMessageVisibilityAsync(changeMsgVisReq);
        }

        public static Task ReturnToQueue(this SQSMessage qmsg) =>
            qmsg.SetVisibilityTimeout(TimeSpan.Zero);

        public static Dictionary<string, MessageAttributeValue> CloneMessageAttributes(
                this Dictionary<string, MessageAttributeValue> messageAttributes,
                params string[] exclusions)
        {
            var output = new Dictionary<string, MessageAttributeValue>();

            foreach(var kvp in messageAttributes)
                if(!exclusions.Contains(kvp.Key))
                    output[kvp.Key] = kvp.Value.CloneMessageAttributeValue();

            return output;
        }

        internal static MessageAttributeValue CloneMessageAttributeValue(this MessageAttributeValue maval)
            => new MessageAttributeValue
            {
                BinaryListValues = maval.BinaryListValues?.Select(x => x).ToList(),
                BinaryValue = maval.BinaryValue,
                DataType = maval.DataType,
                StringListValues = maval.StringListValues?.Select(x => x).ToList(),
                StringValue = maval.StringValue,
            };
    }
}
