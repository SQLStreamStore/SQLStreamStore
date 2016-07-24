namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Concurrent;

    public class Subject<T> : IObserver<T>, IObservable<T>
    {
        private readonly ConcurrentDictionary<Guid, IObserver<T>> _observers = new ConcurrentDictionary<Guid, IObserver<T>>();

        public void OnCompleted()
        {
            foreach(var observer in _observers.Values)
            {
                observer.OnCompleted();
            }
        }

        public void OnError(Exception error)
        {
            foreach(var observer in _observers.Values)
            {
                observer.OnError(error);
            }
        }

        public void OnNext(T value)
        {
            foreach(var observer in _observers.Values)
            {
                observer.OnNext(value);
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            Guid guid = Guid.NewGuid();
            _observers.TryAdd(guid, observer);

            return new DelegateDisposable(() =>
            {
                IObserver<T> _;
                _observers.TryRemove(guid, out _);
            });
        }
    }
}