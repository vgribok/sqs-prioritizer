#nullable enable
using Amazon.CDK;

namespace SqsLongDelay
{
    sealed class Program
    {
        public static void Main()
        {
            var app = new App();
            new SqsLongDelayStack(app);
            app.Synth();
        }
    }
}
