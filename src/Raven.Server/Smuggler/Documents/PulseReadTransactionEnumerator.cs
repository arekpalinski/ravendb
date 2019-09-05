using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Server.Smuggler.Documents
{
    public class PulseReadTransactionEnumerator<T, TState> : IEnumerator<T>
    {
        private readonly IEnumerator<T> _getEnumerator;
        private readonly TState _state;
        private readonly Action<T, TState> _updateState;

        public PulseReadTransactionEnumerator(Func<TState, IEnumerable<T>> getEnumerator, TState state, Action<T, TState> updateState)
        {
            _getEnumerator = getEnumerator(state).GetEnumerator();
            _state = state;
            _updateState = updateState;
        }

        public bool MoveNext()
        {
            if (_getEnumerator.MoveNext() == false)
                return false;

            // if (need to pulse read transaction)
            // TODO
            // 1. PulseTransaction when needed
            // 2. recreate inner iterators by calling _getNewEnumerator
            //


            Current = _getEnumerator.Current;

            _updateState(Current, _state);

            return true;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public T Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        public void PulseTransaction()
        {

        }
    }
}
