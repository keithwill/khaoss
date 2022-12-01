using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;

namespace KHAOSS
{
    /// <summary>
    /// A class for inserting and looking up values
    /// based on a key (like a dictionary) that also
    /// supports looking up matching values by a key
    /// prefix value. Internally implemented with a radix
    /// tree where leaf nodes also store associated values.
    /// No members of this class are thread safe.
    /// </summary>
    /// <seealso>https://en.wikipedia.org/wiki/Radix_tree</seealso>
    /// 
    /// <typeparam name="T"></typeparam>
    public class PrefixLookup<T> where T : class, IEntity
    {

        private Node<T> root;

        private ReaderWriterLockSlim lockSlim= new ReaderWriterLockSlim();

        public PrefixLookup()
        {
            root = new Node<T>(null, Node<T>.GetKeyBytes(string.Empty));
        }

        public int NodeCount => root.GetChildrenCount();

        public int Count => root.GetValuesCount();

        public void Add(string key, T value)
        {
            Span<byte> keyBytes = Encoding.UTF8.GetBytes(key);

            if (lockSlim.IsWriteLockHeld)
            {
                root.SetValue(keyBytes, value);
                return;
            }

            lockSlim.EnterWriteLock();
            try
            {
                root.SetValue(keyBytes, value);
            }
            finally
            {
                lockSlim.ExitWriteLock();
            }
        }


        public void Lock()
        {
            if (lockSlim.IsWriteLockHeld) return;
            lockSlim.EnterWriteLock();
        }

        public void Unlock()
        {
            if (lockSlim.IsWriteLockHeld)
            {
                lockSlim.ExitWriteLock();
            }
        }

        public T Get(string key)
        {
            Span<byte> keyBytes = Encoding.UTF8.GetBytes(key);

            if (lockSlim.IsWriteLockHeld)
            {
                return root.GetValue(keyBytes)?.Value;
            }

            lockSlim.EnterReadLock();
            try
            {
                return root.GetValue(keyBytes)?.Value;

            }
            finally
            {
                lockSlim.ExitReadLock();
            }
        }

        public void Clear()
        {
            lockSlim.EnterWriteLock();
            try
            {
                this.root.Value = null;
                this.root.Children = null;
            }
            finally
            {
                lockSlim.ExitWriteLock();
            }
        }

        public IEnumerable<T> GetByPrefix(string keyPrefix)
        {
            var results = new List<Node<T>>();

            Span<byte> keyPrefixSpan = Encoding.UTF8.GetBytes(keyPrefix);

            if (lockSlim.IsWriteLockHeld)
            {
                if (keyPrefix == string.Empty)
                {
                    root.GetAllValuesAtOrBelow(results);
                }
                else
                {
                    root.GetValuesByPrefix(keyPrefixSpan, results);
                }
                return results.Select(x => x.Value);
            }

            lockSlim.EnterReadLock();
            try
            {
                if (keyPrefix == string.Empty)
                {
                    root.GetAllValuesAtOrBelow(results);
                }
                else
                {
                    root.GetValuesByPrefix(keyPrefixSpan, results);
                }
                return results.Select(x => x.Value);

            }
            finally
            {
                lockSlim.ExitReadLock();
            }


        }

        public void Remove(string key)
        {
            Span<byte> keySpan = Encoding.UTF8.GetBytes(key);
            var node = root.GetValue(keySpan);
            node?.RemoveValue();
        }

    }


}
