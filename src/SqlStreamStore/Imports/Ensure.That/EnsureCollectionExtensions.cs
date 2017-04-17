namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Linq;
    using SqlStreamStore.Imports.Ensure.That.Extensions;

    public static class EnsureCollectionExtensions
    {
        [DebuggerStepThrough]
        public static Param<T> HasItems<T>(this Param<T> param) where T : class, ICollection
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<Collection<T>> HasItems<T>(this Param<Collection<T>> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<ICollection<T>> HasItems<T>(this Param<ICollection<T>> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T[]> HasItems<T>(this Param<T[]> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null)
                throw ExceptionFactory.CreateForParamNullValidation(param, ExceptionMessages.Common_IsNotNull_Failed);

            if (param.Value.Length < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<List<T>> HasItems<T>(this Param<List<T>> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IList<T>> HasItems<T>(this Param<IList<T>> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IDictionary<TKey, TValue>> HasItems<TKey, TValue>(this Param<IDictionary<TKey, TValue>> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null || param.Value.Count < 1)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_HasItemsFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T[]> SizeIs<T>(this Param<T[]> param, int expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Length != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Length));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T[]> SizeIs<T>(this Param<T[]> param, long expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Length != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Length));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> SizeIs<T>(this Param<T> param, int expected) where T : ICollection
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> SizeIs<T>(this Param<T> param, long expected) where T : ICollection
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<ICollection<T>> SizeIs<T>(this Param<ICollection<T>> param, int expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<ICollection<T>> SizeIs<T>(this Param<ICollection<T>> param, long expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IList<T>> SizeIs<T>(this Param<IList<T>> param, int expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IList<T>> SizeIs<T>(this Param<IList<T>> param, long expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IDictionary<TKey, TValue>> SizeIs<TKey, TValue>(this Param<IDictionary<TKey, TValue>> param, int expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IDictionary<TKey, TValue>> SizeIs<TKey, TValue>(this Param<IDictionary<TKey, TValue>> param, long expected)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Count != expected)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_SizeIs_Failed.Inject(expected, param.Value.Count));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IDictionary<TKey, TValue>> ContainsKey<TKey, TValue>(this Param<IDictionary<TKey, TValue>> param, TKey key)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.ContainsKey(key))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_ContainsKey_Failed.Inject(key));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<Dictionary<TKey, TValue>> ContainsKey<TKey, TValue>(this Param<Dictionary<TKey, TValue>> param, TKey key)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.ContainsKey(key))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_ContainsKey_Failed.Inject(key));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<IList<T>> Any<T>(this Param<IList<T>> param, Func<T, bool> predicate)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.Any(predicate))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_Any_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<List<T>> Any<T>(this Param<List<T>> param, Func<T, bool> predicate)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.Any(predicate))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_Any_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<ICollection<T>> Any<T>(this Param<ICollection<T>> param, Func<T, bool> predicate)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.Any(predicate))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_Any_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<Collection<T>> Any<T>(this Param<Collection<T>> param, Func<T, bool> predicate)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.Any(predicate))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_Any_Failed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T[]> Any<T>(this Param<T[]> param, Func<T, bool> predicate)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.Any(predicate))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Collections_Any_Failed);

            return param;
        }
    }
}