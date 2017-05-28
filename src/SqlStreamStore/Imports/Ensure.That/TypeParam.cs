#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.Imports.Ensure.That
{
    using System;

    public class TypeParam : Param
    {
        public readonly Type Type;

        public TypeParam(string name, Type type)
            : base(name)
        {
            Type = type;
        }
    }
}