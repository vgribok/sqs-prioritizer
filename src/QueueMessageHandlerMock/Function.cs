#nullable enable
using System.Text.Json;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using System;

using SqsDelay;
using aws_sdk_extensions;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace QueueMessageHandlerMock
{
    public class Function
    {
        internal class Message
        {
            public string Text { get; set; } = string.Empty;
            public bool? Throw { get; set; }
            /// <summary>
            /// Format like: "1s", "3m15s", "2h" <see cref="Duration"/>
            /// </summary>
            public string? VisiblityTimeoutDuration { get; set; }
        }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function() {}

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            IEnumerable<Task> tasks = evnt.Records.Select(record => ProcessMessageAsync(record, context.Logger));
            return Task.WhenAll(tasks);
        }

        /// <summary>
        /// A fake/mock job processor that can "process" the message and throw an exception. 
        /// It can also change message visibility timeout.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaLogger logger)
        {
            logger.LogLine($"Received message: \"{message.Body}\"");
            Message payload = JsonSerializer.Deserialize<Message>(message.Body);

            if(!string.IsNullOrWhiteSpace(payload.VisiblityTimeoutDuration))
            {
                TimeSpan visiblityTimeout = Duration.ToTimeSpan(payload.VisiblityTimeoutDuration);
                await message.SetVisibilityTimeout(visiblityTimeout);
                logger.LogLine($"Successfully set message visibility timeout to {visiblityTimeout.ToDuration()}");
            }

            if (payload.Throw ?? false)
                throw new Exception($"Requested exception: \"{payload.Text}\"");

            logger.LogLine($"Processed payload: \"{payload.Text}\"");
        }
    }
}
