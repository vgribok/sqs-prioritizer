using Amazon;

namespace SqsProcessorContainer.Models
{
    public class AppSettings
    {
        public string QeueueArn{ get; set; } = string.Empty;

        public int ProcessorCount { get; set; }

        public Arn QueueArnParsed => Arn.Parse(this.QeueueArn);
    }
}
