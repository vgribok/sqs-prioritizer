using Amazon.SQS;
using Amazon.SQS.Model;
using MessagePrioritizer.Models;
using Microsoft.Extensions.Logging;

namespace MessagePrioritizer
{
    /// <summary>
    /// Runs periodic check on the depth of the output queue
    /// and toggles fetch pause event temporarily stopping prioritizer 
    /// message pump processors from moving messages from source queues
    /// to the output queue, preserving priority in the process.
    /// </summary>
    internal class OutputQueueDepthMonitor
    {
        private readonly string outputQueueUrl;
        private readonly int queueDepthCheckFrequencyMilliseconds;
        private readonly IAmazonSQS sqsClient;
        private readonly ILogger<OutputQueueDepthMonitor> logger;
        private readonly int throttleQueueDepth;

        internal bool IsOutputQueuePaused { get; set; } = false;

        internal event Action<bool>? OnOutputQueuePaused;

        public OutputQueueDepthMonitor(OutputQueueSettings outputQueueSettings, ILogger<OutputQueueDepthMonitor> logger, IAmazonSQS sqsClient)
        {
            this.outputQueueUrl = outputQueueSettings.OutputQueueUrl;
            this.queueDepthCheckFrequencyMilliseconds = outputQueueSettings.QueueDepthCheckFrequencyMilliseconds;
            this.sqsClient = sqsClient;
            this.logger = logger;
            this.throttleQueueDepth = outputQueueSettings.ThrottleQueueDepth;
        }

        public async Task RunMonitoringLoop(CancellationToken cancellationToken)
        {
            using (this.logger.BeginScope("Output queue depth monitor {OutputQueueUrl}", this.outputQueueUrl))
            {
                do
                {
                    await CheckOutputQueueDepthAndNotifyPrioritizerMessagePump();
                } while (!cancellationToken.WaitHandle.WaitOne(this.queueDepthCheckFrequencyMilliseconds));
            }

            this.logger.LogInformation("Exited output queue depth monitoring for {OutputQueueUrl}", this.outputQueueUrl);
        }

        internal async Task CheckOutputQueueDepthAndNotifyPrioritizerMessagePump()
        {
            bool needsToBePaused = false;
            int currentOutputQueueDepth = -1;
            try
            {
                currentOutputQueueDepth = await this.GetOutputQueueDepth();
                needsToBePaused = currentOutputQueueDepth >= this.throttleQueueDepth;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Failed to check output queue depth. Output queue will be allowed to grow indefinitely");
            }
            finally
            {
                this.TriggerPauseEventIfNeeded(needsToBePaused, currentOutputQueueDepth);
            }
        }

        private void TriggerPauseEventIfNeeded(bool needsToBePaused, int outputQueueDepth)
        {
            if (needsToBePaused == this.IsOutputQueuePaused)
                return;
            
            this.IsOutputQueuePaused = needsToBePaused;

            try
            {
                this.logger.LogInformation("Need to *{OutputQueuePauseAction}* fetching messages from source queues due to output queue depth reaching {OutputQueueDepth}, while pause threshold is {ThrottleQueueDepth}",
                                needsToBePaused ? "pause" : "resume",
                                outputQueueDepth, this.throttleQueueDepth);

                // Trigger the event to let message pump change its fetching behavior
                this.OnOutputQueuePaused?.Invoke(needsToBePaused);
            }
            catch(Exception ex)
            {
                this.logger.LogError(ex, "Processor \"Pause event\" hander has thrown an exception");
            }
        }

        private async Task<int> GetOutputQueueDepth()
        {
            using (this.logger.BeginScope("Checking queue depth"))
            {
                var attReq = new GetQueueAttributesRequest
                {
                    QueueUrl = this.outputQueueUrl
                };

                attReq.AttributeNames.Add("ApproximateNumberOfMessages");
                GetQueueAttributesResponse response = await this.sqsClient.GetQueueAttributesAsync(attReq);
                int queueDepth = response.ApproximateNumberOfMessages;
                this.logger.LogTrace("Queue depth: {OutputQueueDepth}", queueDepth);
                return queueDepth;
            }
        }
    }
}
