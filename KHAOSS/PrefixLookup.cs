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
            var keyBytes = Node<T>.GetKeyBytes(key);
            root.SetValue(keyBytes, 0, value);
        }

        public T Get(string key)
        {
            var keyBytes = Node<T>.GetKeyBytes(key);
            return root.GetValue(keyBytes, 0)?.Value;
        }

        public void Clear()
        {
            this.root.Value = null;
            this.root.Children = null;
        }

        public IEnumerable<KeyValuePair<string, T>> GetByPrefix(string keyPrefix, bool sort = false)
        {
            if (!sort)
            {
                return GetByPrefixUnsorted(keyPrefix);
            }

            return GetByPrefixSorted(keyPrefix);

        }
        private List<Node<T>> results = new();
        private IEnumerable<KeyValuePair<string, T>> GetByPrefixUnsorted(string keyPrefix)
        {
            results.Clear();
            var keyPrefixLength = Encoding.UTF8.GetBytes(keyPrefix, keyPrefixBuffer);
            var keyPrefixSpan = new Span<byte>(keyPrefixBuffer, 0, keyPrefixLength);

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
            var keyPrefixBytes = Node<T>.GetKeyBytes(key);
            var node = root.GetValue(keyPrefixBytes, 0);
            node?.RemoveValue();
        }

    }


}
