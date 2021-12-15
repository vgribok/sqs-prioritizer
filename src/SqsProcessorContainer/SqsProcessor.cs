#nullable enable

using SqsDelay;
using System.Text.Json;

using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

using aws_sdk_extensions;
using System.Text;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Base class for container-friendly, thread-safe, long-polling but cancellable SQS message processor.
    /// </summary>
    /// <typeparam name="TMsgModel"></typeparam>
    public abstract class SqsProcessor<TMsgModel>
    {
        // TODO: Use ILogger instead of Console methods.

        protected readonly string _queueArn;
        protected readonly string _queueUrl;
        protected readonly RegionEndpoint _awsregion;
        protected readonly int _longPollingWaitSeconds;
        protected readonly int _listenerNumber;

        public static List<TProcessor> StartProcessors<TProcessor>(int listenerCount, Func<int, TProcessor> factory) 
            where TProcessor : SqsProcessor<TMsgModel>
                => Enumerable.Range(1, listenerCount)
                    .Select(i => factory(i))
                    .ToList();

        public SqsProcessor(int listenerNumber)
        {
            _queueArn = "arn:aws:sqs:us-east-2:444039259723:delme";
            _listenerNumber = listenerNumber;
            _queueUrl = MiscExtensions.SqsArnToUrl(_queueArn);
            _awsregion = RegionEndpoint.GetBySystemName(Arn.Parse(_queueArn).Region);
            _longPollingWaitSeconds = 2;
        }

        private AmazonSQSClient GetSqsClient() => new AmazonSQSClient(_awsregion);

        public async Task Listen(CancellationToken appExitRequestToken)
        {
            Console.WriteLine($"Started listener {_listenerNumber}.");

            try
            {
                for (int i = 1 ; ; i++)
                    await FetchFromQueue(appExitRequestToken, i);
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine($"Queue processor {_listenerNumber} is terminated by cancellation request");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Message listener {_listenerNumber} threw exception and stopped: { ex.Message}");
            }
        }

        private async Task FetchFromQueue(CancellationToken token, int step)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 1, // Helps to avoid sequencing of processors and shifts parallelism control to the number of created SqsProcessor class instances
                WaitTimeSeconds = _longPollingWaitSeconds
            };

            using var sqsClient = GetSqsClient();
            var receiveMsgReponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest, token);
            var messages = receiveMsgReponse.Messages;

            var text = new StringBuilder();
            text.Append($"Listener {_listenerNumber} Step {step}: ");

            if (messages.Count == 0)
            {
                text.Append("No Messages to process");
                Console.WriteLine(text);
                return;
            }
    
            text.Append($"Processing a batch of {messages.Count} messages");
            Console.WriteLine(text);

            IEnumerable<Task> processors = from message in messages
                                            select ProcessMessageAsync(message);
            await Task.WhenAll(processors);
        }

        /// <summary>
        /// A fake/mock job processor that can "process" the message and throw an exception. 
        /// It can also change message visibility timeout.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task ProcessMessageAsync(Message message)
        {
            Console.WriteLine($"Listener {_listenerNumber} Received message with receipt {message.GetReceiptTail()} and body: \"{message.Body}\"");
            TMsgModel payload = JsonSerializer.Deserialize<TMsgModel>(message.Body)!;

            await ProcessPayload(message.ReceiptHandle, payload);
            await DeleteMessageAsync(message.ReceiptHandle);
        }

        protected abstract Task ProcessPayload(string receiptHandle, TMsgModel payload);

        protected async Task UpdateMessageVisibilityTimeout(string receiptHandle, TimeSpan visibilityTimeout)
        {
            await SqsMessageExtensions.SetVisibilityTimeout(_queueArn, receiptHandle, visibilityTimeout);
            Console.WriteLine($"Listener {_listenerNumber} successfully set message visibility timeout to {visibilityTimeout.ToDuration()}");
        }

        private async Task DeleteMessageAsync(string receiptHandle)
        {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = _queueUrl,
                ReceiptHandle = receiptHandle
            };
            using var sqsClient = GetSqsClient();
            DeleteMessageResponse response = await sqsClient.DeleteMessageAsync(deleteMessageRequest);
            
            Console.WriteLine($"Listener {_listenerNumber} deleted message with receipt: \"{SqsMessageExtensions.GetReceiptTail(receiptHandle)}\"");
        }
    }
}
