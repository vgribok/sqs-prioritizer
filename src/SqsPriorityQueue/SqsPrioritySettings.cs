﻿using Amazon;

namespace SqsPriorityQueue
{
    public class SqsPrioritySettings
    {
        /// <summary>
        /// Comma-delimited list or queue ARNs.
        /// Queue priorities are set from the first being highest to the last being lowest.
        /// </summary>
        public string QueueArns{ get; set; } = string.Empty;

        /// <summary>
        /// The number of parallel pollers/processors.
        /// Each processor handles all queues in the order of priority.
        /// If messages in each batch take roughly same time to process, it may be
        /// beneficial to keep processor count at 1 and batch messages instead.
        /// </summary>
        public int ProcessorCount { get; set; } = 3;

        /// <summary>
        /// Maximum number of messages in the batch. Should not exceed 10.
        /// Should be kept at 1 with multiple parallel processors if messages in the batch require wide range or processing time.
        /// </summary>
        public int MessageBatchSize { get; set; } = 1;

        /// <summary>
        /// Amount of time the highest priority queue is long-polled for.
        /// This value is ignored if there is only one queue, in which case max long polling time of 20s is used.
        /// With multiple priority queues longer time will result in better $$ economy due to 
        /// reduced number of SQS requests, but will result in delays of processing lower-priority queues.
        /// </summary>
        public int HighPriorityWaitTimeoutSeconds { get; set; } = 3;

        /// <summary>
        /// On message processing failure, the message will be reappear 
        /// in the queue after this interval.
        /// </summary>
        public int VisibilityTimeoutOnProcessingFailureSeconds { get; set; } = 1;

        #region Computed Properties

        public IEnumerable<string> QueueArnCollection => from arnString in QueueArns.Split(',')
                                                         let arn = arnString.Trim()
                                                         where !string.IsNullOrWhiteSpace(arn)
                                                         select arn;

        public IEnumerable<Arn> QueueArnsParsed => QueueArnCollection.Select(Arn.Parse);

        #endregion Computed Properties
    }
}
