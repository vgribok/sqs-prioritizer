﻿#nullable enable

using SqsDelay;
using System.Text.Json;

using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

using aws_sdk_extensions;
using Microsoft.Extensions.Logging;

namespace SqsProcessorContainer
{
    /// <summary>
    /// Base class for container-friendly, thread-safe, long-polling but cancellable SQS message processor.
    /// </summary>
    /// <typeparam name="TMsgModel">Queue message model</typeparam>
    public abstract class SqsProcessor<TMsgModel>
    {
        public const int maxLongPollingTimeSeconds = 20;

        protected readonly Arn[] _queueArns;
        protected readonly string[] _queueUrls;
        protected readonly RegionEndpoint _awsregion;
        protected readonly int _longPollingWaitSeconds;
        protected readonly string _listenerId;
        protected readonly int _highPriorityWaitTimeoutSeconds;
        protected readonly TimeSpan _failureVisibilityTimeout;
        protected readonly ILogger logger;

        public SqsProcessor(Arn[] sqsQueueArns, string listenerId, ILogger logger, int highPriorityWaitTimeoutSeconds, int failureVisibilityTimeoutSeconds)
        {
            this.logger = logger;
            _queueArns = sqsQueueArns;
            _listenerId = listenerId;
            _highPriorityWaitTimeoutSeconds = highPriorityWaitTimeoutSeconds;
            _failureVisibilityTimeout = TimeSpan.FromSeconds(failureVisibilityTimeoutSeconds);

            _queueUrls = _queueArns.Select(qarn => qarn.SqsArnToUrl()).ToArray();
            _awsregion = RegionEndpoint.GetBySystemName(GetQueueRegion(_queueArns));
            _longPollingWaitSeconds = 20; // Have it as long as possible
        }

        private static string GetQueueRegion(Arn[] queueArns)
        {
            string[] regions = queueArns.Select(arn => arn.Region).Distinct().ToArray();
            if (regions.Length > 1)
                throw new ArgumentException($"All queues must belong to the same region", nameof(queueArns));
            return regions[0];
        }

        private AmazonSQSClient GetSqsClient() => new(_awsregion);

        protected string Id(int queueIndex) => $"Queue {queueIndex} Listener {_listenerId}";

        public async Task Listen(CancellationToken appExitRequestToken)
        {
            logger.LogInformation($"Started listener loop for {_listenerId}.");

            try
            {
                while(!appExitRequestToken.IsCancellationRequested)
                    await FetchAndProcess(appExitRequestToken);
            }
            catch (TaskCanceledException)
            {
                logger.LogInformation($"Queue processor {_listenerId} is terminated by cancellation request");
            }
            catch (Exception ex)
            {
                logger.LogInformation($"Message listener {_listenerId} threw exception and stopped: { ex.Message}");
                throw;
            }
        }

        private async Task FetchAndProcess(CancellationToken cancellationToken)
        {
            for (int queueIndex = 0; queueIndex < _queueArns.Length; queueIndex++) // Going from the highest priority 
            {
                bool isTopPriorityQueue = queueIndex == 0 && _queueArns.Length > 1;

                if (isTopPriorityQueue)
                {   // A top priority queue with lower priority queues present
                    bool mayHaveMessages = true;
                    for (int pollingDelaySeconds = _highPriorityWaitTimeoutSeconds ; mayHaveMessages ; pollingDelaySeconds = 0)
                    {
                        mayHaveMessages = await FetchAndProcess(queueIndex, pollingDelaySeconds, cancellationToken, maxMessages: 1);
                    }
                }else
                {   // A single queue or a lower priority queue

                    // If there's only one queue, use maximum long-polling delay.
                    // If it's a low priority queue, we'll do short polling
                    int pollingDelaySeconds = _queueArns.Length == 1 ? maxLongPollingTimeSeconds : 0; 
                    
                    if (await FetchAndProcess(queueIndex, pollingDelaySeconds, cancellationToken, maxMessages: 1))
                        // processed messages
                        break; // restart processing loop from the highest priority as it may
                    // No messages were processed, continue to the lower priority queue
                }
            }
        }

        private async Task<bool> FetchAndProcess(int queueIndex, int pollingDelaySeconds, CancellationToken cancellationToken, int maxMessages = 1)
        {
            List<Message> messages = await FetchMessagesFromSingleQueueWithLongPoll(
                        cancellationToken,
                        _queueUrls[queueIndex],
                        pollingDelaySeconds,
                        maxMessages
                    );
            return await ProcessMessages(messages, queueIndex);
        }

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
                    CancellationToken cancellationToken, string queueUrl, int longPollTimeSeconds = 20, int maxMessages = 1)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = maxMessages, // Helps to avoid sequencing of processors and shifts parallelism control to the number of created SqsProcessor class instances
                WaitTimeSeconds = longPollTimeSeconds
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
        private async Task ProcessMessage(Message message, int queueIndex)
        {
            logger.LogDebug($"{Id(queueIndex)} Received message with id {message.MessageId} and body: \"{message.Body}\"");
            try
            {
                try
                {
                    TMsgModel payload = JsonSerializer.Deserialize<TMsgModel>(message.Body)!;
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

        protected virtual Task HandlePayloadProcessingException(Exception ex, Message message, int queueIndex, TimeSpan failureVisibilityTimeoutSeconds)
        {
            logger.LogError($"{Id(queueIndex)} Failed to process message {message.MessageId} " +
                            $"due to \"{ex.Message}\". " +
                            $"Its visibility timeout is set to {failureVisibilityTimeoutSeconds.ToDuration()}");

            logger.LogDebug($"{Id(queueIndex)} message: {message}\ncaused exception {ex}\nand will be retruned to the queue or to DLQ");

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