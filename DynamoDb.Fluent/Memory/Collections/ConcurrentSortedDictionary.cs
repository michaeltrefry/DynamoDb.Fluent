using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using DynamoDb.Fluent.Memory.Definitions;

namespace DynamoDb.Fluent.Memory.Collections
{
    public class ConcurrentSortedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        #region Variables

        private readonly ReaderWriterLockSlim readWriteLock = new ReaderWriterLockSlim();
        private readonly SortedDictionary<TKey, TValue> dictionary;

        #endregion

        #region Constructors

        public ConcurrentSortedDictionary()
        {
            dictionary = new SortedDictionary<TKey, TValue>();
        }

        public ConcurrentSortedDictionary(IComparer<TKey> comparer)
        {
            dictionary = new SortedDictionary<TKey, TValue>(comparer);
        }

        public ConcurrentSortedDictionary(IDictionary<TKey, TValue> dictionary)
        {
            this.dictionary = new SortedDictionary<TKey, TValue>(dictionary);
        }

        public ConcurrentSortedDictionary(IDictionary<TKey, TValue> dictionary, IComparer<TKey> comparer)
        {
            this.dictionary = new SortedDictionary<TKey, TValue>(dictionary, comparer);
        }

        #endregion

        #region Properties

        public IComparer<TKey> Comparer
        {
            get 
            {
                readWriteLock.EnterReadLock();
                try
                {
                    return dictionary.Comparer;
                }
                finally
                {
                    readWriteLock.ExitReadLock();
                }
            }
        }

        public int Count
        {
            get
            {
                readWriteLock.EnterReadLock();
                try
                {
                    return dictionary.Count;
                }
                finally
                {
                    readWriteLock.ExitReadLock();
                }
            }
        }

        public TValue this[TKey key]
        { 
            get
            {
                readWriteLock.EnterReadLock();
                try
                {
                    return dictionary[key];
                }
                finally
                {
                    readWriteLock.ExitReadLock();
                }
            }
            set
            {
                readWriteLock.EnterWriteLock();
                try
                {
                    dictionary[key] = value;
                }
                finally
                {
                    readWriteLock.ExitWriteLock();
                }
            }
        }

        public SortedDictionary<TKey, TValue>.KeyCollection Keys
        {
            get
            {
                readWriteLock.EnterReadLock();
                try
                {
                    return new SortedDictionary<TKey, TValue>.KeyCollection(dictionary);
                }
                finally
                {
                    readWriteLock.ExitReadLock();
                }
            }
        }

        public SortedDictionary<TKey, TValue>.ValueCollection Values
        {
            get
            {
                readWriteLock.EnterReadLock();
                try
                {
                    return new SortedDictionary<TKey, TValue>.ValueCollection(dictionary);
                }
                finally
                {
                    readWriteLock.ExitReadLock();
                }
            }
        }

        #endregion

        #region Methods

        public void Add(TKey key, TValue value)
        {
            readWriteLock.EnterWriteLock();
            try
            {
                dictionary.Add(key, value);
            }
            finally
            {
                readWriteLock.ExitWriteLock();
            }
        }

        public void Clear()
        {
            readWriteLock.EnterWriteLock();
            try
            {
                dictionary.Clear();
            }
            finally
            {
                readWriteLock.ExitWriteLock();
            }
        }

        public bool ContainsKey(TKey key)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.ContainsKey(key);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public bool ContainsValue(TValue value)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.ContainsValue(value);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            readWriteLock.EnterReadLock();
            try
            {
                dictionary.CopyTo(array, index);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public override bool Equals(Object obj)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.Equals(obj);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new SafeEnumerator<KeyValuePair<TKey, TValue>>(dictionary.GetEnumerator(), readWriteLock);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new SafeEnumerator<KeyValuePair<TKey, TValue>>(dictionary.GetEnumerator(), readWriteLock);
        }

        public override int GetHashCode()
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.GetHashCode();
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public bool Remove(TKey key)
        {
            readWriteLock.EnterWriteLock();
            try
            {
                return dictionary.Remove(key);
            }
            finally
            {
                readWriteLock.ExitWriteLock();
            }
        }

        public override string ToString()
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.ToString();
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (!TryGetValue(key, out var value))
            {
                value = valueFactory(key);
                TryAdd(key, value);
            }
            return value;
        }
        

        public bool TryGetValue(TKey key, out TValue value)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.TryGetValue(key, out value);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }
        
        public bool TryUpdate(TKey key, TValue value)
        {
            readWriteLock.EnterReadLock();
            try
            {
                if (!dictionary.ContainsKey(key)) 
                    return false;
                dictionary[key] = value;
                return true;
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.TryAdd(key, value);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }
        
        public bool TryRemove(TKey key, out TValue value)
        {
            readWriteLock.EnterReadLock();
            try
            {
                return dictionary.TryGetValue(key, out value) && dictionary.Remove(key);
            }
            finally
            {
                readWriteLock.ExitReadLock();
            }
        }
        
        #endregion
    }
}


