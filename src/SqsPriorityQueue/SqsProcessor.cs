﻿#nullable enable

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
        protected readonly RegionEndpoint _awsregion;
        protected readonly int _highPriorityWaitTimeoutSeconds;
        protected readonly TimeSpan _failureVisibilityTimeout;
        protected readonly int _messageBatchSize;
        protected readonly ILogger logger;

        public string? ListenerId { get; set; }

        public SqsProcessor(Arn[] sqsQueueArns, ILogger logger, 
                int highPriorityWaitTimeoutSeconds, int failureVisibilityTimeoutSeconds, int messageBatchSize)
        {
            this.logger = logger;
            _queueArns = sqsQueueArns;
            _highPriorityWaitTimeoutSeconds = highPriorityWaitTimeoutSeconds;
            _failureVisibilityTimeout = TimeSpan.FromSeconds(failureVisibilityTimeoutSeconds);
            _messageBatchSize = messageBatchSize;

            _queueUrls = _queueArns.Select(qarn => qarn.SqsArnToUrl()).ToArray();
            _awsregion = RegionEndpoint.GetBySystemName(GetQueueRegion(_queueArns));
        }

        private static string GetQueueRegion(Arn[] queueArns)
        {
            string[] regions = queueArns.Select(arn => arn.Region).Distinct().ToArray();
            if (regions.Length > 1)
                throw new ArgumentException($"All queues must belong to the same region", nameof(queueArns));
            return regions[0];
        }

        private AmazonSQSClient GetSqsClient() => new(_awsregion);

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

            logger.LogInformation($"Started listener loop for {Id}.");

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
                logger.LogInformation($"Queue processor {Id} is terminated by cancellation request");
            }
            catch (Exception ex)
            {
                logger.LogInformation($"Message listener {Id} threw exception and stopped: { ex.Message}");
                throw;
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
                bool isTopPriorityQueue = queueIndex == 0 && _queueArns.Length > 1;

                if (isTopPriorityQueue)
                {   // A top priority queue with lower priority queues present
                    bool mayHaveMessages = true;

                    for (int pollingDelaySeconds = lowPriorityQueueMayHaveMessages ? 0 : _highPriorityWaitTimeoutSeconds; // Do long polling of high-prty queue only if we think low-prty are empty, short-poll otherwise.
                        mayHaveMessages && !cancellationToken.IsCancellationRequested ; 
                        pollingDelaySeconds = 0) // keep pulling messages from top priority queue for as long there are ones
                    {
                        mayHaveMessages = await FetchAndProcessQueueMessageBatch(queueIndex, pollingDelaySeconds, cancellationToken);
                    }
                }else
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

        private async Task<bool> FetchAndProcessQueueMessageBatch(int queueIndex, int pollingDelaySeconds, CancellationToken cancellationToken)
        {
            List<Message> messages = await FetchMessagesFromSingleQueueWithLongPoll(cancellationToken, queueIndex, pollingDelaySeconds);
            return await ProcessMessages(messages, queueIndex);
        }

        /// <summary>
        /// Returns true of there were messages to process.
        /// Returns false if input message collection was empty.
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="queueIndex"></param>
        /// <returns></returns>
        private async Task<bool> ProcessMessages(List<Message> messages, int queueIndex)
        {
            if (messages.Count == 0)
            {
                logger.LogDebug($"{Id(queueIndex)}: polling cycle returned no messages.");
                return false;
            }

            IEnumerable<Task> processors = messages.Select(m => ProcessMessage(m, queueIndex));
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

            using var sqsClient = GetSqsClient();
            var receiveMsgReponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
            return receiveMsgReponse.Messages;
        }

        protected virtual TMsgModel DeserializeMessage(string messageBody)
            => JsonSerializer.Deserialize<TMsgModel>(messageBody)!;

        /// <summary>
        /// A fake/mock job processor that can "process" the message and throw an exception. 
        /// It can also change message visibility timeout.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private async Task ProcessMessage(Message message, int queueIndex)
        {
            logger.LogDebug($"{Id(queueIndex)} Received message with id {message.MessageId} and body: \"{message.Body}\"");
            try
            {
                try
                {
                    TMsgModel payload = this.DeserializeMessage(message.Body); 
                    await ProcessPayload(payload, message.ReceiptHandle, queueIndex, message.MessageId);
                }catch (Exception ex)
                {
                    throw new PayloadProcessingException(ex);
                }
                await DeleteMessageAsync(message, queueIndex);
            }
            catch (PayloadProcessingException ex)
            {
                await HandlePayloadProcessingException(ex, message, queueIndex, _failureVisibilityTimeout);
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
            logger.LogError($"{Id(queueIndex)} Failed to process message {message.MessageId} " +
                            $"due to \"{ex.Message}\". " +
                            $"Its visibility timeout is set to {failureVisibilityTimeoutSeconds.ToDuration()}");

            logger.LogDebug($"{Id(queueIndex)} message: {message}\ncaused exception {ex}\nand will be returned to the queue or to DLQ");

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
        protected abstract Task ProcessPayload(TMsgModel payload, string receiptHandle, int queueIndex, string messageId);

        protected async Task UpdateMessageVisibilityTimeout(string receiptHandle, int queueIndex, TimeSpan visibilityTimeout)
        {
            await SqsMessageExtensions.SetVisibilityTimeout(_queueArns[queueIndex], receiptHandle, visibilityTimeout);
            logger.LogDebug($"{Id(queueIndex)} successfully set message visibility timeout to {visibilityTimeout.ToDuration()}");
        }

        protected async Task DeleteMessageAsync(Message message, int queueIndex)
        {
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = _queueUrls[queueIndex],
                ReceiptHandle = message.ReceiptHandle
            };
            using var sqsClient = GetSqsClient();
            DeleteMessageResponse response = await sqsClient.DeleteMessageAsync(deleteMessageRequest);
            
            logger.LogDebug($"{Id(queueIndex)} deleted message with Id: \"{message.MessageId}\"");
        }
    }
}