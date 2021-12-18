using Amazon;

namespace SqsProcessorContainer.Models
{
    public class AppSettings
    {
        public string QueueArns{ get; set; } = string.Empty;

        public int ProcessorCount { get; set; }

        public int HighPriorityWaitTimeoutSeconds { get; set; } = 3;

        /// <summary>
        /// On message processing failure, the message will be reappear 
        /// in the queue after this interval.
        /// </summary>
        public int VisibilityTimeoutOnProcessingFailureSeconds { get; set; } = 1;

        #region Computed Properties

        public IEnumerable<string> QueueArnCollection => QueueArns.Split(',').Select(x => x.Trim());

        public IEnumerable<Arn> QueueArnsParsed => QueueArnCollection.Select(Arn.Parse);

        #endregion Computed Properties
    }
}
