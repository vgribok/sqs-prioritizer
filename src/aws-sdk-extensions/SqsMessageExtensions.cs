#nullable enable
using System;
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
            SetVisibilityTimeout(qmsg.EventSourceArn, qmsg.ReceiptHandle, delay);

        public static async Task SetVisibilityTimeout(string queueArn, string messageReceiptHandle, TimeSpan delay)
        {
            var changeMsgVisReq = new ChangeMessageVisibilityRequest
            {
                QueueUrl = MiscExtensions.SqsArnToUrl(queueArn),
                ReceiptHandle = messageReceiptHandle,
                VisibilityTimeout = (int)delay.TotalSeconds
            };

            using var client = new AmazonSQSClient(RegionEndpoint.GetBySystemName(Arn.Parse(queueArn).Region));
            await client.ChangeMessageVisibilityAsync(changeMsgVisReq);
        }

        public static Task ReturnToQueue(this SQSMessage qmsg) =>
            qmsg.SetVisibilityTimeout(TimeSpan.Zero);

        public static string GetReceiptTail(this Message qmsg, int length = 10)
            => GetReceiptTail(qmsg.ReceiptHandle, length);

        public static string GetReceiptTail(string receiptHandle, int length = 10)
            => receiptHandle.Substring(receiptHandle.Length - length);
    }
}
