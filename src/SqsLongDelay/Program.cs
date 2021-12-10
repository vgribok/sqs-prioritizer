#nullable enable
using Amazon.CDK;

namespace SqsDelay
{
    sealed class Program
    {
        public static void Main()
        {
            var app = new App();
            new SqsDelayStack(app);
            app.Synth();
        }
    }
}
