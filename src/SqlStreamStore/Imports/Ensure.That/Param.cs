#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.Imports.Ensure.That
{
    using System;

    public abstract class Param
    {
        public const string DefaultName = "";

        public readonly string Name;

        protected Param(string name)
        {
            Name = name ?? DefaultName;
        }
    }

    public class Param<T> : Param
    {
        public readonly T Value;
        public Func<Param<T>, string> ExtraMessageFn;
        public Func<Param<T>, Exception> ExceptionFn;

        public Param(string name, T value) : base(name)
        {
            Value = value;
        }
    }
}