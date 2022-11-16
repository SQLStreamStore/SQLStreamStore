namespace SqlStreamStore
{
    using System;

    public class GapHandlingSettings
    {
        public GapHandlingSettings(long minimumWarnTime, long skipTime)
        {
            if(minimumWarnTime >= skipTime)
                throw new ArgumentException("'MinimumWarnTime' is not allowed to be bigger or equal then 'SkipTime'");

            MinimumWarnTime = minimumWarnTime;
            SkipTime = skipTime;
        }

        /// <summary>
        /// The time that needs to pass before we start logging that transactions are taking longer than expected.
        /// </summary>
        public long MinimumWarnTime { get; }

        /// <summary>
        /// The time that needs to pass before we allow to move on with subscribing and live with the fact that we might have skipped an event.
        /// </summary>
        public long SkipTime { get; }
    }
}