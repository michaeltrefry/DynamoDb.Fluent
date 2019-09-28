using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace DynamoDb.Fluent.Memory.Definitions
{
    public class SafeEnumerator<T> : IEnumerator<T>
    {
        #region Variables

        // this is the (thread-unsafe)
        // enumerator of the underlying collection
        private readonly IEnumerator<T> enumerator;

        // this is the object we shall lock on. 
        private readonly ReaderWriterLockSlim lockSlim;

        #endregion 

        #region Constructor

        public SafeEnumerator(IEnumerator<T> inner, ReaderWriterLockSlim readWriteLockSlim)
        {
            enumerator = inner;
            lockSlim = readWriteLockSlim;

            // Enter lock in constructor
            lockSlim.EnterReadLock();
        }

        #endregion

        #region Implementation of IDisposable

        public void Dispose()
        {
            // .. and exiting lock on Dispose()
            // This will be called when the foreach loop finishes
            lockSlim.ExitReadLock();
        }

        #endregion

        #region Implementation of IEnumerator

        // we just delegate actual implementation
        // to the inner enumerator, that actually iterates
        // over some collection

        public bool MoveNext()
        {
            return enumerator.MoveNext();
        }

        public void Reset()
        {
            enumerator.Reset();
        }

        public T Current => enumerator.Current;

        object IEnumerator.Current => Current;

        #endregion
    }
}