#nullable enable

using SqsDelay;
using System.Text.Json;

using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

using aws_sdk_extensions;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Base class for container-friendly, thread-safe, long-polling but cancellable SQS message processor.
    /// </summary>
    /// <typeparam name="TMsgModel">Queue message model</typeparam>
    public abstract class SqsProcessor<TMsgModel>
    {
        // TODO: Use ILogger instead of Console methods.

        protected readonly Arn _queueArn;
        protected readonly string _queueUrl;
        protected readonly RegionEndpoint _awsregion;
        protected readonly int _longPollingWaitSeconds;
        protected readonly string _listenerId;

        public static List<TProcessor> StartProcessors<TProcessor>(int listenerCount, Func<int, TProcessor> factory) 
            where TProcessor : SqsProcessor<TMsgModel>
                => Enumerable.Range(1, listenerCount)
                    .Select(i => factory(i))
                    .ToList();

        public SqsProcessor(Arn sqsQueueArn, string listenerId)
        {
            _queueArn = sqsQueueArn;
            _listenerId = listenerId;

            _queueUrl = _queueArn.SqsArnToUrl();
            _awsregion = RegionEndpoint.GetBySystemName(_queueArn.Region);
            _longPollingWaitSeconds = 20; // Have it as long as possible
        }

        private AmazonSQSClient GetSqsClient() => new(_awsregion);

        public async Task Listen(CancellationToken appExitRequestToken)
        {
            Console.WriteLine($"Started listener {_listenerId}.");

            try
            {
                while(true)
                    await FetchAndProcess(appExitRequestToken);
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine($"Queue processor {_listenerId} is terminated by cancellation request");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Message listener {_listenerId} threw exception and stopped: { ex.Message}");
                throw;
            }
        }

        private async Task FetchAndProcess(CancellationToken cancellationToken)
        {
            List<Message> messages = await FetchMessageBatch(cancellationToken);

            if (messages.Count == 0)
            {
                Console.WriteLine($"Listener {_listenerId}: polling cycle returned no messages.");
                return;
            }

            IEnumerable<Task> processors = messages.Select(ProcessMessage);
            await Task.WhenAll(processors);
        }

        private async Task<List<Message>> FetchMessageBatch(CancellationToken cancellationToken)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 1, // Helps to avoid sequencing of processors and shifts parallelism control to the number of created SqsProcessor class instances
                WaitTimeSeconds = _longPollingWaitSeconds
            };

            using var sqsClient = GetSqsClient();
            var receiveMsgReponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
            return receiveMsgReponse.Messages;
        }

        /// <summary>
        /// A fake/mock job processor that can "process" the message and throw an exception. 
        /// It can also change message visibility timeout.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task ProcessMessage(Message message)
        {
            Console.WriteLine($"Listener {_listenerId} Received message with receipt {message.GetReceiptTail()} and body: \"{message.Body}\"");
            TMsgModel payload = JsonSerializer.Deserialize<TMsgModel>(message.Body)!;

            await ProcessPayload(message.ReceiptHandle, payload);
            await DeleteMessageAsync(message.ReceiptHandle);
        }

        /// <summary>
        /// Method to override in subclasses
        /// </summary>
        /// <param name="receiptHandle">Can be used to extend or shrink current message visibility timeout</param>
        /// <param name="payload">Message payload</param>
        /// <returns></returns>
        protected abstract Task ProcessPayload(string receiptHandle, TMsgModel payload);

        protected async Task UpdateMessageVisibilityTimeout(string receiptHandle, TimeSpan visibilityTimeout)
        {
            await SqsMessageExtensions.SetVisibilityTimeout(_queueArn, receiptHandle, visibilityTimeout);
            Console.WriteLine($"Listener {_listenerId} successfully set message visibility timeout to {visibilityTimeout.ToDuration()}");
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
            
            Console.WriteLine($"Listener {_listenerId} deleted message with receipt: \"{SqsMessageExtensions.GetReceiptTail(receiptHandle)}\"");
        }
    }
}
