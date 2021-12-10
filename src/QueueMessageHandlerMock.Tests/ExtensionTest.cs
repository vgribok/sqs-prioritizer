using System;
using System.Collections.Generic;
using Xunit;

using SqsDelay;
using aws_sdk_extensions;

namespace QueueMessageHandlerMock.Tests
{
    public class ExtensionTest
    {
        [Fact]
        public void TestDurationParsing()
        {
            var map = new Dictionary<string, TimeSpan>
            {
                [string.Empty] = TimeSpan.Zero,
                ["0.00s"] = TimeSpan.Zero,
                ["0d0.00s"] = TimeSpan.Zero,
                ["0s"] = TimeSpan.Zero,
                [".0s"] = TimeSpan.Zero,
                ["0d0h0m"] = TimeSpan.Zero,
                ["1s"] = TimeSpan.FromSeconds(1),
                [".12345s"] = TimeSpan.FromSeconds(.12345),
                ["1234D"] = TimeSpan.FromDays(1234),
                ["1d"] = TimeSpan.FromDays(1),
                ["1234H"] = TimeSpan.FromHours(1234),
                ["5h"] = TimeSpan.FromHours(5),
                ["1M"] = TimeSpan.FromMinutes(1),
                ["2345m"] = TimeSpan.FromMinutes(2345),
                ["3d12h37m24.25s"] = new TimeSpan(3, 12, 37, 24, 250),
            };

            foreach (var item in map)
            {
                TimeSpan span = Duration.ToTimeSpan(item.Key);
                Assert.True(item.Value == span, item.Key);
            }
        }

        [Fact]
        public void TestConversionToDuration()
        {
            var map = new Dictionary<TimeSpan, string>
            {
                [TimeSpan.Zero] = "0s",
                [TimeSpan.FromSeconds(1)] = "1s",
                [TimeSpan.FromSeconds(.12345)] = "0.123s",
                [TimeSpan.FromDays(1234)] = "1234d",
                [TimeSpan.FromDays(1)] = "1d",
                [TimeSpan.FromHours(1234)] = "51d10h",
                [TimeSpan.FromHours(5)] = "5h",
                [TimeSpan.FromMinutes(1)] = "1m",
                [TimeSpan.FromMinutes(2345)] = "1d15h5m",
                [new TimeSpan(3, 12, 37, 24, 250)] = "3d12h37m24.250s",
            };

            foreach (var item in map)
            {
                string actual = item.Key.ToDuration();
                Assert.True(actual == item.Value, $"Span: {item.Key}, actual: \"{actual}\", expected: \"{item.Value}\"");
            }
        }

        [Fact]
        public void TestSqsArnToUrl()
        {
            string actual = MiscExtensions.SqsArnToUrl("arn:aws:sqs:us-east-2:123456789012:1st-one");
            Assert.Equal("https://sqs.us-east-2.amazonaws.com/123456789012/1st-one", actual);
        }
    }
}
