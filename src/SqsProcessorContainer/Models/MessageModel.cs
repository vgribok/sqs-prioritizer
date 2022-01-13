namespace SqsProcessorContainer
{
    internal class MessageModel
    {
        public string Text { get; set; } = string.Empty;

        public bool? Throw { get; set; }

        public bool? Return { get; set; }

        /// <summary>
        /// Format like: "1s", "3m15s", "2h" <see cref="Duration"/>
        /// </summary>
        public string? Timeout { get; set; }
    }
}
