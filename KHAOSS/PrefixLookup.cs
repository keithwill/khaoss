﻿using System;
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

        public PrefixLookup()
        {
            root = new Node<T>( null, Node<T>.GetKeyBytes(string.Empty) );
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
            
            var keyPrefixBytes = Node<T>.GetKeyBytes(keyPrefix);

            IEnumerable<Node<T>> matchingNodesWithValues;

            if (keyPrefix == string.Empty)
            {
                matchingNodesWithValues = root.GetAllValuesAtOrBelow();
            }
            else
            {
                matchingNodesWithValues = root.GetValuesByPrefix(keyPrefixBytes, 0);
            }

            var sb = new StringBuilder();
            var keyNodes = new List<Node<T>>();
            var results = matchingNodesWithValues?.Select(x => {
                sb.Clear();
                keyNodes.Clear();
                var parent = x.Parent;
                keyNodes.Add(x);
                while(parent != null)
                {
                    keyNodes.Add(parent);
                    parent = parent.Parent;
                }
                for (int i = keyNodes.Count - 1; i > -1; i--)
                {
                    var nodeLabel = keyNodes[i].Label;
                    sb.Append(nodeLabel);
                }
                return new KeyValuePair<string, T>(sb.ToString(), x.Value!);
            });
            if (sort)
            {
                results = results.OrderBy(x => x.Key);
            }
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