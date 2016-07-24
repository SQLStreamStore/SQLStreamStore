namespace SqlStreamStore.Infrastructure
{
    using System;

    internal static class ObservableExtensions
    {
        internal static IDisposable Subscribe<T>(this IObservable<T> source, Action<T> onNext)
        {
            return source.Subscribe(new AnonymousObserver<T>(onNext));
        }
    }
}