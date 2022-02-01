#nullable enable

using SqsDelay;
using System.Text.Json;

using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

using aws_sdk_extensions;
using Microsoft.Extensions.Logging;
using SqsPriorityQueue;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Base class for container-friendly, thread-safe, long-polling and cancellable SQS message processor
    /// for multiple priority-based SQS queues.
    /// </summary>
    /// <typeparam name="TMsgModel">Queue message model</typeparam>
    public abstract class SqsProcessor<TMsgModel> : IPriorityQueueProcessor
    {
        public const int maxLongPollingTimeSeconds = 20;

        protected readonly Arn[] _queueArns;
        protected readonly string[] _queueUrls;
        protected readonly RegionEndpoint[] _queueRegions;
        protected readonly int _highPriorityWaitTimeoutSeconds;
        protected readonly TimeSpan _failureVisibilityTimeout;
        protected readonly int _messageBatchSize;
        private readonly IAmazonSQS _sqsClient;
        protected readonly ILogger logger;

        public string? ListenerId { get; set; }

        public SqsProcessor(SqsPrioritySettings settings, ILogger logger, IAmazonSQS sqsClient)
        {
            this.logger = logger;
            
            _queueArns = settings.QueueArnsParsed.ToArray();
            if (_queueArns.Length == 0)
                throw new ArgumentNullException(nameof(settings.QueueArnCollection), "Queue ARN collection cannot be empty.");

            _highPriorityWaitTimeoutSeconds = settings.HighPriorityWaitTimeoutSeconds;
            _failureVisibilityTimeout = TimeSpan.FromSeconds(settings.VisibilityTimeoutOnProcessingFailureSeconds);
            _messageBatchSize = settings.MessageBatchSize;
            _sqsClient = sqsClient;

            _queueUrls = _queueArns.Select(qarn => qarn.SqsArnToUrl()).ToArray();
            _queueRegions = _queueArns.Select(qarn => RegionEndpoint.GetBySystemName(qarn.Region)).ToArray();
        }
        protected string Id(int queueIndex) => $"Queue {queueIndex} Listener {this.ListenerId}";

        /// <summary>
        /// Main message polling and processing loop.
        /// </summary>
        /// <param name="appExitRequestToken">Application/container exit signal propagator</param>
        /// <returns></returns>
        public async Task Listen(CancellationToken appExitRequestToken)
        {
            if(this.ListenerId == null)
                throw new ArgumentNullException(nameof(this.ListenerId), "Listener Id must be set before listening loop is started.");

            using (logger.BeginScope("ProcessorId={ProcessorId}", this.ListenerId))
            {
                logger.LogInformation("Started listening loop for source queues: {QueueArnsCommaDelimited}",
                    string.Join(",", _queueArns.AsEnumerable()));

                try
                {
                    for (bool firstSweep = true;
                        !appExitRequestToken.IsCancellationRequested;
                        firstSweep = false)
                    {
                        await FetchAndProcessAllPrioritiesSequentially(firstSweep, appExitRequestToken);
                    }
                }
                catch (TaskCanceledException)
                {
                    logger.LogInformation("Queue processor {ProcessorId} is terminated by cancellation request", this.ListenerId);
                }
                catch (Exception ex)
                {
                    logger.LogInformation("Message processor {ProcessorId} threw an exception and stopped: {Error}", this.ListenerId, ex.Message);
                    throw;
                }
            }
        }

        /// <summary>
        /// Implements priority-based message processing by checking messages from top priority
        /// to lowest. This approach ensures that higher priority messages are always processed
        /// ahead of lower priority messages.
        /// The drawback of this approach is that it employs short or zero polling delays
        /// resulting in elevated number of SQS requests, affecting SQS cost.
        /// </summary>
        /// <param name="lowPriorityQueueMayHaveMessages">Should be set to true on the first sweep, false on all subsequent.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// What this methods does:
        /// - Key requirement: ensures that messages from higher-priority queue are *always* picked for processing before messages from lower priority queues.
        /// - Queue are short-polled for as long as there are messages in them. Once all queues go empty, highest priority queue is long-polled.
        /// - Having to short-polled low-priority queues results in burning SQS requests. This is the price to pay for having priority queues done with SQS.
        /// See project README.MD for details of trade-offs.
        /// </remarks>
        private async Task FetchAndProcessAllPrioritiesSequentially(bool lowPriorityQueueMayHaveMessages, CancellationToken cancellationToken)
        {
            // Going from the highest priority 
            for (int queueIndex = 0; 
                queueIndex < _queueArns.Length && !cancellationToken.IsCancellationRequested; 
                queueIndex++) 
            {
                using (logger.BeginScope("QueueIndex={QueueIndex}, QueueArn={QueueArn}", queueIndex, _queueArns[queueIndex]))
                {
                    bool isTopPriorityQueue = queueIndex == 0 && _queueArns.Length > 1;

                    if (isTopPriorityQueue)
                    {   // A top priority queue with lower priority queues present
                        bool mayHaveMessages = true;

                        for (int pollingDelaySeconds = lowPriorityQueueMayHaveMessages ? 0 : _highPriorityWaitTimeoutSeconds; // Do long polling of high-prty queue only if we think low-prty are empty, short-poll otherwise.
                            mayHaveMessages && !cancellationToken.IsCancellationRequested;
                            pollingDelaySeconds = 0) // keep pulling messages from top priority queue for as long there are ones
                        {
                            mayHaveMessages = await FetchAndProcessQueueMessageBatch(queueIndex, pollingDelaySeconds, cancellationToken);
                        }
                    }
                    else
                    {   // A single queue or a lower priority queue

                        // If there's only one queue, use maximum long-polling delay.
                        // If it's a low priority queue, we'll do short polling
                        int pollingDelaySeconds = _queueArns.Length == 1 ? maxLongPollingTimeSeconds : 0;

                        lowPriorityQueueMayHaveMessages = await FetchAndProcessQueueMessageBatch(queueIndex, pollingDelaySeconds, cancellationToken);

                        if (lowPriorityQueueMayHaveMessages)
                        {
                            // processed some messages
                            queueIndex = -1; // Restart from the highest-priority queue without breaking the loop
                        }
                        // No messages were processed, continue to the lower priority queue
                    }
                }
            }
        }

        private async Task<bool> FetchAndProcessQueueMessageBatch(int queueIndex, int pollingDelaySeconds, CancellationToken cancellationToken)
        {
            List<Message> messages = await FetchMessagesFromSingleQueueWithLongPoll(cancellationToken, queueIndex, pollingDelaySeconds);
            return await ProcessMessages(messages, queueIndex, cancellationToken);
        }

        /// <summary>
        /// Returns true of there were messages to process.
        /// Returns false if input message collection was empty.
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="queueIndex"></param>
        /// <returns></returns>
        private async Task<bool> ProcessMessages(List<Message> messages, int queueIndex, CancellationToken cancellationToken)
        {
            if (messages.Count == 0)
            {
                logger.LogDebug("Polling cycle returned no messages.");
                return false;
            }

            IEnumerable<Task> processors = messages.Select(m => ProcessMessage(m, queueIndex, cancellationToken));
            await Task.WhenAll(processors);
            return true;
        }

        private async Task<List<Message>> FetchMessagesFromSingleQueueWithLongPoll(
                    CancellationToken cancellationToken, int queueIndex, int longPollTimeSeconds)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = _queueUrls[queueIndex],
                MaxNumberOfMessages = _messageBatchSize, // Helps to avoid sequencing of processors and shifts parallelism control to the number of created SqsProcessor class instances
                WaitTimeSeconds = longPollTimeSeconds
            };

            var receiveMsgReponse = await _sqsClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
            return receiveMsgReponse.Messages;
        }

        /// <summary>
        /// Default implementation uses <see cref="JsonSerializer"/> to convert JSON into strongly-typed object.
        /// </summary>
        /// <param name="messageBody"></param>
        /// <returns></returns>
        protected virtual TMsgModel DeserializeMessage(string messageBody)
            => JsonSerializer.Deserialize<TMsgModel>(messageBody)!;

        /// <summary>
        /// Default implementation uses <see cref="JsonSerializer"/> to convert object to JSON.
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        protected virtual string SerializeMessage(TMsgModel model)
            => model!.GetType() == typeof(string) ? (model as string)! : JsonSerializer.Serialize(model);

        /// <summary>
        /// A fake/mock job processor that can "process" the message and throw an exception. 
        /// It can also change message visibility timeout.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task ProcessMessage(Message message, int queueIndex, CancellationToken cancellationToken)
        {
            using(logger.BeginScope("SqsMessageId={SqsMessageId}, SqsMessageBody={SqsMessageBody}", message.MessageId, message.Body))
            {
                logger.LogDebug("Received message");
                try
                {
                    TMsgModel payload = this.DeserializeMessage(message.Body); 
                    await ProcessPayload(payload, cancellationToken, message.ReceiptHandle, queueIndex, message.MessageId);
                }
                catch (Exception ex)
                {
                    // Failed to process the message.
                    // Default implementation of HandlePayloadProcessingException() will return failed
                    // message back to the queue (or to the DQL) by setting message visibility timeout to _failureVisibilityTimeout.
                    await HandlePayloadProcessingException(ex, message, queueIndex, _failureVisibilityTimeout);
                    return;
                }

                // Message processed successfully.
                // Delete message from the queue after successful processing.
                await DeleteMessageAsync(message, queueIndex);
            }
        }

        /// <summary>
        /// Provides ability to handle message processing errors.
        /// Default behavior is to put the message back into the queue for a re-try 
        /// after the delay of <see cref="_failureVisibilityTimeout"/>.
        /// </summary>
        /// <param name="ex">Exception thrown during message processing.</param>
        /// <param name="message">SQS message being processed</param>
        /// <param name="queueIndex">Queue index/priority</param>
        /// <param name="failureVisibilityTimeoutSeconds">Configuration setting value for failed message visibility timeout.</param>
        /// <returns></returns>
        protected virtual Task HandlePayloadProcessingException(Exception ex, Message message, int queueIndex, TimeSpan failureVisibilityTimeoutSeconds)
        {
            logger.LogError(ex, "Failed to process message due to {Error}. Its visibility timeout is set to {VisibilityTimeout}",
                            ex.Message, failureVisibilityTimeoutSeconds.ToDuration());

            logger.LogDebug("Message caused exception and will be returned to the queue for re-processing, or to DLQ: {Error}", ex.Message);

            // Returns message to either to the original queue or to a DLQ. If former, the message will be
            // re-tried after delay of failureVisibilityTimeoutSeconds.
            return UpdateMessageVisibilityTimeout(message.ReceiptHandle, queueIndex, failureVisibilityTimeoutSeconds);
        }

        /// <summary>
        /// Method to override in subclasses
        /// </summary>
        /// <param name="payload">Message payload</param>
        /// <param name="receiptHandle">Can be used to extend or shrink current message visibility timeout</param>
        /// <param name="queueIndex"></param>
        /// <param name="messageId"></param>
        /// <returns></returns>
        protected abstract Task ProcessPayload(TMsgModel payload, CancellationToken cancellationToken, string receiptHandle, int queueIndex, string messageId);

        protected async Task UpdateMessageVisibilityTimeout(string receiptHandle, int queueIndex, TimeSpan visibilityTimeout)
        {
            await SqsMessageExtensions.SetVisibilityTimeout(_queueArns[queueIndex], receiptHandle, visibilityTimeout);
            logger.LogDebug("Successfully set message visibility timeout to {VisibilityTimeout}", visibilityTimeout.ToDuration());
        }

        protected async Task DeleteMessageAsync(Message message, int queueIndex)
        {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = _queueUrls[queueIndex],
                ReceiptHandle = message.ReceiptHandle
            };
            DeleteMessageResponse response = await _sqsClient.DeleteMessageAsync(deleteMessageRequest);
            
            logger.LogDebug("Message deleted from the queue");
        }

        /// <summary>
        /// Re-sends message to the queue where it was picked from.
        /// </summary>
        /// <param name="messageBody">Message body, could be modified since last pick</param>
        /// <param name="queueIndex"></param>
        /// <param name="delaySeconds">Optional delay before message re-appears in the queue</param>
        /// <returns></returns>
        /// <remarks>
        /// To ensure no message duplications, please ensure that original message is not returned back by throwing an exception.
        /// </remarks>
        protected async Task<SendMessageResponse> ReturnMessageToQueue(TMsgModel payload, int queueIndex, int delaySeconds = 0)
        {
            var sendMessageRequest = new SendMessageRequest
            {
                QueueUrl = _queueUrls[queueIndex],
                MessageBody = this.SerializeMessage(payload),
                DelaySeconds = delaySeconds
            };
            
            var response = await _sqsClient.SendMessageAsync(sendMessageRequest);
            
            logger.LogDebug("Returned message to the queue with the delay of {DelaySeconds} seconds.", delaySeconds);
            
            return response;
        }
    }
}
