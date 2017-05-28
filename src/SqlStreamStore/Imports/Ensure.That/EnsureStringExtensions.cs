#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;
    using System.Text.RegularExpressions;
    using SqlStreamStore.Imports.Ensure.That.Extensions;

    public static class EnsureStringExtensions
    {
        [DebuggerStepThrough]
        public static Param<string> IsNotNullOrWhiteSpace(this Param<string> param)
        {
            if (!Ensure.IsActive)
                return param;

            if(param.Value == null)
                throw ExceptionFactory.CreateForParamNullValidation(param, ExceptionMessages.Common_IsNotNull_Failed);

            if (string.IsNullOrWhiteSpace(param.Value))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsNotNullOrWhiteSpace_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> IsNotNullOrEmpty(this Param<string> param)
        {
            if (param.Value == null)
                throw ExceptionFactory.CreateForParamNullValidation(param, ExceptionMessages.Common_IsNotNull_Failed);

            if (string.IsNullOrEmpty(param.Value))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsNotNullOrEmpty_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> IsNotEmpty(this Param<string> param)
        {
            if (string.Empty.Equals(param.Value))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsNotEmpty_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> HasLengthBetween(this Param<string> param, int minLength, int maxLength)
        {
            if (param.Value == null)
                throw ExceptionFactory.CreateForParamNullValidation(param, ExceptionMessages.Common_IsNotNull_Failed);

            var length = param.Value.Length;

            if (length < minLength)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_HasLengthBetween_Failed_ToShort.Inject(minLength, maxLength, length));

            if (length > maxLength)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_HasLengthBetween_Failed_ToLong.Inject(minLength, maxLength, length));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> Matches(this Param<string> param, string match)
        {
            return Matches(param, new Regex(match));
        }

        [DebuggerStepThrough]
        public static Param<string> Matches(this Param<string> param, Regex match)
        {
            if (!match.IsMatch(param.Value))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_Matches_Failed.Inject(param.Value, match));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> SizeIs(this Param<string> param, int expected)
        {
            if (param.Value.Length != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_SizeIs_Failed.Inject(expected, param.Value.Length));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> IsEqualTo(this Param<string> param, string expected, StringComparison? comparison = null)
        {
            if (!StringEquals(param.Value, expected, comparison))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsEqualTo_Failed.Inject(param.Value, expected));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> IsNotEqualTo(this Param<string> param, string expected, StringComparison? comparison = null)
        {
            if (StringEquals(param.Value, expected, comparison))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsNotEqualTo_Failed.Inject(param.Value, expected));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<string> IsGuid(this Param<string> param)
        {
            Guid guid;
            if (!Guid.TryParse(param.Value, out guid))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Strings_IsGuid_Failed.Inject(param.Value));

            return param;
        }

        private static bool StringEquals(string x, string y, StringComparison? comparison = null)
        {
            return comparison.HasValue
                ? string.Equals(x, y, comparison.Value)
                : string.Equals(x, y);
        }
    }
}
