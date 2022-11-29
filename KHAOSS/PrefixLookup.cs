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
    public class PrefixLookup<T> where T : class
    {

        private Node<T> root;

        private byte[] keyReadBuffer = new byte[1_000_000];
        private byte[] keyPrefixBuffer = new byte[1_000_000];

        public PrefixLookup()
        {
            root = new Node<T>(null, Node<T>.GetKeyBytes(string.Empty));
        }

        public int NodeCount => root.GetChildrenCount();

        public int Count => root.GetValuesCount();

        public void Add(string key, T value)
        {
            var keyBytes = GetKeyAsUtf8ByteSpan(key);
            root.SetValue(keyBytes, value);
        }

        public T Get(string key)
        {
            var keySpan = GetKeyAsUtf8ByteSpan(key);
            return root.GetValue(keySpan)?.Value;
        }

        public void Clear()
        {
            this.root.Value = null;
            this.root.Children = null;
        }

        public IEnumerable<KeyValuePair<string, T>> GetKeyValuePairByPrefix(string keyPrefix, bool sort = false)
        {
            if (!sort)
            {
                return GetByPrefixUnsorted(keyPrefix);
            }

            return GetByPrefixSorted(keyPrefix);

        }

        public IEnumerable<T> GetByPrefixValues(string keyPrefix)
        {
            var results = new List<Node<T>>();

            var keyPrefixSpan = GetKeyAsUtf8ByteSpan(keyPrefix);

            if (keyPrefix == string.Empty)
            {
                root.GetAllValuesAtOrBelow(results);
            }
            else
            {
                root.GetValuesByPrefix(keyPrefixSpan, results);
            }
            foreach(var value in results)
            {
                yield return value.Value;
            }
        }

        private IEnumerable<KeyValuePair<string, T>> GetByPrefixUnsorted(string keyPrefix)
        {
            var results = new List<Node<T>>();
            
            var keyPrefixSpan = GetKeyAsUtf8ByteSpan(keyPrefix);

            if (keyPrefix == string.Empty)
            {
                root.GetAllValuesAtOrBelow(results);
            }
            else
            {
                root.GetValuesByPrefix(keyPrefixSpan, results);
            }

            foreach(var node in results)
            {
                int keyByteLength = node.KeySegment.Length;
                var parent = node.Parent;
                while (parent != null)
                {
                    keyByteLength += parent.KeySegment.Length;
                    parent = parent.Parent;
                }
                var keyBytes = new Span<byte>(keyReadBuffer, 0, keyByteLength);
                parent = node.Parent;
                int offset = 0;
                while (parent != null)
                {
                    if (parent.KeySegment.Length == 1)
                    {
                        keyBytes[offset] = parent.KeySegment[0];
                        offset++;
                    }
                    if (parent.KeySegment.Length > 1)
                    {
                        for (int i = parent.KeySegment.Length - 1; i >= 0; i--)
                        {
                            keyBytes[offset + i] = parent.KeySegment[i];
                        }
                        offset += parent.KeySegment.Length;
                    }
                    parent = parent.Parent;
                }
                keyBytes.Reverse();
                var key = Encoding.UTF8.GetString(keyBytes);
                yield return new KeyValuePair<string, T>(key, node.Value);
            }
        }

        private IEnumerable<KeyValuePair<string, T>> GetByPrefixSorted(string keyPrefix)
        {
            var results = GetByPrefixUnsorted(keyPrefix);
            results = results.OrderBy(x => x.Key);
            return results;
        }

        public void Remove(string key)
        {
            var keySpan = GetKeyAsUtf8ByteSpan(key);
            var node = root.GetValue(keySpan);
            node?.RemoveValue();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetKeyAsUtf8ByteSpan(string key)
        {
            var keyPrefixLength = Encoding.UTF8.GetBytes(key, keyPrefixBuffer);
            var keyPrefixSpan = new Span<byte>(keyPrefixBuffer, 0, keyPrefixLength);
            return keyPrefixSpan;
        }

    }


}
