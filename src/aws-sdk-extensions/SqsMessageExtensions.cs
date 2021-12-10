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
        public static async Task SetVisibilityTimeout(this SQSMessage qmsg, TimeSpan delay)
        {
            var changeMsgVisReq = new ChangeMessageVisibilityRequest
            {
                QueueUrl = MiscExtensions.SqsArnToUrl(qmsg.EventSourceArn),
                ReceiptHandle = qmsg.ReceiptHandle,
                VisibilityTimeout = (int)delay.TotalSeconds
            };

            using var client = new AmazonSQSClient();
            await client.ChangeMessageVisibilityAsync(changeMsgVisReq);
        }

        public static Task ReturnToQueue(this SQSMessage qmsg) =>
            qmsg.SetVisibilityTimeout(TimeSpan.Zero);
    }
}
