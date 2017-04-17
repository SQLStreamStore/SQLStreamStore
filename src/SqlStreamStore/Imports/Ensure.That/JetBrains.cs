using System;

#pragma warning disable 1591
// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedParameter.Local
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable IntroduceOptionalParameters.Global
// ReSharper disable MemberCanBeProtected.Global
// ReSharper disable InconsistentNaming

// ReSharper disable once CheckNamespace
namespace JetBrains.Annotations
{
    /// <summary>
    /// Indicates that IEnumarable, passed as parameter, is not enumerated.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public sealed class NoEnumerationAttribute : Attribute { }
}