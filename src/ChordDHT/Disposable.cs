using System;

namespace ChordDHT
{
    public static class Disposable
    {
        class EmptyDisposable : IDisposable
        {
            public void Dispose() { }
        }

        class ActionDisposable : IDisposable
        {
            private readonly Action _action;

            public ActionDisposable(Action action)
            {
                _action = action;
            }

            public void Dispose() => _action();
        }

        public static readonly IDisposable Empty = new EmptyDisposable();
        public static IDisposable Create(Action action) => new ActionDisposable(action);

        public static IDisposable Sequence(params IDisposable[] disposables) =>
            Create(() =>
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            });
    }
}