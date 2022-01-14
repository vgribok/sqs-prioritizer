namespace SqsPriorityQueue
{
    /// <summary>
    /// DI-friendly interface for SQS priority processors.
    /// </summary>
    public interface IPriorityQueueProcessor
    {
        /// <summary>
        /// Typically listener priority number, but can be anything.
        /// Used mostly in logging for identifying a processor among 
        /// multiple parallel ones.
        /// </summary>
        public string? ListenerId { get; set; }

        /// <summary>
        /// Can be used to start processor main listening loop.
        /// </summary>
        /// <param name="appExitRequestToken"></param>
        /// <returns></returns>
        public Task Listen(CancellationToken appExitRequestToken);
    }
}
