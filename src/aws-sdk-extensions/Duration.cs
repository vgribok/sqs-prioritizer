#nullable enable
using System;
using System.Text.RegularExpressions;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace SqsLongDelay
{
    /// <summary>
    /// Enables simplified representation of the <see cref="TimeSpan"/> value.
    /// Examples "1d12h", "155m", "2m24.5s", "2h".
    /// </summary>
    /// <remarks>
    /// Please note that only seconds allow fractions. All other segments are integer.
    /// </remarks>
    public static class Duration
    {
        private static readonly string parserPattern = @"^ (?:(?<Days>\d+)[dD])?    (?: (?<Hours>\d+)[hH])?    (?:(?<Minutes>\d+)[mM])?   (?: (?<Seconds>\d+)?  (?<Fractions> \.\d*)?[sS])? $";
        private static readonly Regex durationParser = new Regex(parserPattern, RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace | RegexOptions.Singleline);

        public static TimeSpan ToTimeSpan(string duration)
        {
            if(string.IsNullOrWhiteSpace(duration))
                return TimeSpan.Zero;

            var match = durationParser.Match(duration);
            if (!match.Success)
                throw new ArgumentException($"Invalid duration \"{duration}\"", nameof(duration));

            IEnumerable<Group> groups = match.Groups; 

            TimeSpan span = TimeSpan.Zero;

            foreach(Group group in groups.Where(g => g.Success))
            {
                TimeSpan durationSpan;

                switch(group.Name)
                {
                    case "Days":
                        double days = double.Parse(group.Value);
                        durationSpan = TimeSpan.FromDays(days);
                        break;
                    case "Hours":
                        double hours = double.Parse(group.Value);
                        durationSpan = TimeSpan.FromHours(hours);
                        break;
                    case "Minutes":
                        double minutes = double.Parse(group.Value);
                        durationSpan = TimeSpan.FromMinutes(minutes);
                        break;
                    case "Seconds":
                    case "Fractions":
                        double seconds = double.Parse(group.Value);
                        durationSpan = TimeSpan.FromSeconds(seconds);
                        break;
                    default:
                        durationSpan = TimeSpan.Zero;
                        break;
                }
                span = span.Add(durationSpan);
            }

            return span;
        }

        /// <summary>
        /// Returns short duration string like "1h13m". Can be parsed using <see cref="ToTimeSpan(string)"/>
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static string ToDuration(this TimeSpan span)
        {
            var duration = new StringBuilder();

            if(span.Days > 0)
                duration.Append($"{span.Days}d");
            if (span.Hours > 0)
                duration.Append($"{span.Hours}h");
            if (span.Minutes > 0)
                duration.Append($"{span.Minutes}m");

            if (span.Seconds > 0 || span.Milliseconds > 0 || duration.Length == 0)
            {
                duration.Append(span.Seconds);
                if (span.Milliseconds > 0)
                    duration.Append($".{span.Milliseconds / 1000.0}");
                duration.Append('s');
            }

            return duration.ToString();
        }
    }
}
