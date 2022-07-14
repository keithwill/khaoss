using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace KHAOSS
{
    public class Node<T> where T : class
    {
        public Node<T> Parent = null;
        public byte[] KeySegment;

        public Node<T>[] Children;
        public T Value;


        public Node()
        {
            this.KeySegment = GetKeyBytes("");
        }

        public Node(Node<T> parent, byte[] label)
        {
            this.Parent = parent;
            this.KeySegment = label;
        }

        public void SetValue(byte[] key, int startingCharacter, T value)
        {

            var keySegmentToSetLength = key.Length - startingCharacter;
            //var setKey = System.Text.Encoding.UTF8.GetString(key, startingCharacter, keySegmentToSetLength);
            //var keySegmentString = System.Text.Encoding.UTF8.GetString(KeySegment);
            //Console.WriteLine($"Set '{setKey}' on '{keySegmentString}'");


            // Set on a child
            var matchingChild = FindMatchingChild(key, startingCharacter, out var matchingLength);
            if (matchingChild != null)
            {
                //Console.WriteLine($"'{setKey}' matched {matchingLength} bytes on child '{matchingChild.Label}'");
                if (matchingLength == keySegmentToSetLength)
                {
                    if (matchingLength == matchingChild.KeySegment.Length)
                    {
                        // We found a child node that matches our key exactly
                        // E.g. Key = "apple" and child key = "apple"
                        matchingChild.Value = value;                        
                    }
                    else
                    {

                        // https://en.wikipedia.org/wiki/Radix_tree#/media/File:Inserting_the_word_'team'_into_a_Patricia_trie_with_a_split.png

                        // We matched the whole set key, but not the entire child key. We need to split the child key
                        //Console.WriteLine($"\t Splitting label '{matchingChild.Label}' to {matchingChild.Label.Substring(matchingLength)}");
                        matchingChild.SplitKeySegmentAtLength(matchingLength);
                        matchingChild.SetValue(key, matchingLength, value);
                    }

                }
                else
                {
                    // We matched part of the set key on a child
                    if (matchingLength == matchingChild.KeySegment.Length)
                    {
                        // and the entire child key
                        matchingChild.SetValue(key, startingCharacter + matchingLength, value);
                    }
                    else
                    {
                        // and only part of the child key
                        //Console.WriteLine($"\t Splitting label '{matchingChild.Label}' to {matchingChild.Label.Substring(matchingLength)}");
                        matchingChild.SplitKeySegmentAtLength(matchingLength);
                        matchingChild.SetValue(key, startingCharacter + matchingLength, value);
                    }                    
                }
            }
            else
            {
                // There were no matching children. 
                // E.g. Key = "apple" and no child that even starts with 'a'. Add a new child node
                //Console.WriteLine($"Creating '{setKey}' on '{Label}'");
                var keySegment = GetKeySegment(key, startingCharacter);
                var newChild = new Node<T>(this, keySegment);
                newChild.Value = value;

                AddChild(newChild);
            }

        }

        private void AddChild(Node<T> newChild)
        {
            if (Children == null)
            {
                Children = new Node<T>[1];
            }
            else
            {
                Array.Resize(ref Children, Children.Length + 1);
            }
            Children[^1] = newChild;
        }

        public static byte[] GetKeySegment(byte[] key, int startingCharacter)
        {
            var remainingLength = key.Length - startingCharacter;
            var keySegment = new byte[remainingLength];
            Array.Copy(key, startingCharacter, keySegment, 0, remainingLength);
            return keySegment;
        }

        public void SplitKeySegmentAtLength(int startingCharacter)
        {
            // Create new split child
            var newChildKeySegment = GetKeySegment(KeySegment, startingCharacter);
            var newChild = new Node<T>(this, newChildKeySegment) { Value = this.Value };

            this.Value = null;
            if (Children == null) {
                AddChild(newChild);
            }
            else
            {

                //push existing children down
                newChild.Children = Children;
                for(int i = 0; i < newChild.Children.Length; i++)
                {
                    Children[i].Parent = newChild;
                }
                this.Children = null;
                AddChild(newChild);
            }

            // Change this nodes segment to portion that wasn't cutoff
            var newKeySegment = new byte[startingCharacter];
            Array.Copy(KeySegment, 0, newKeySegment, 0, newKeySegment.Length);
            KeySegment = newKeySegment;

        }

        public Node<T> GetValue(byte[] key, int startingCharacter)
        {
            if (Children == null) return null;
            var remainingKeyLength = key.Length - startingCharacter;

            foreach(var child in Children)
            {
                var matchingBytes = GetMatchingBytes(key, startingCharacter, child.KeySegment);
                   
                if (matchingBytes > 0)
                {
                    if (matchingBytes == remainingKeyLength)
                    {
                        if (matchingBytes == child.KeySegment.Length)
                        {
                            // We found a key with an exact match
                            return child;
                        }
                        else
                        {
                            // We found a key that was longer than the
                            // one we were looking for that matched the length of the key

                            // In a radix tree, that means our key wasn't found, because if it
                            // existed, it would have been split at our length
                            return null;
                        }
                    }
                    else if (matchingBytes < remainingKeyLength)
                    {
                        return child.GetValue(key, startingCharacter + matchingBytes);
                    }    
                }
            }
            return null;
        }

        public void GetValuesByPrefix(Span<byte> key, List<Node<T>> results)
        {
            if (Children != null)
            {
                foreach(var child in Children)
                {
                    var matchingBytes = GetMatchingBytes(key, child.KeySegment);
                    if (matchingBytes > 0)
                    {
                        if (matchingBytes == key.Length)
                        {
                            // We found a key that matched the entire prefix,
                            // either exactly or at least to the length of the search key
                            child.GetAllValuesAtOrBelow(results);
                        }
                        else if (matchingBytes < key.Length)
                        {
                            ;
                            child.GetValuesByPrefix(key.Slice(matchingBytes), results);
                        }      
                    }
                }
            }
        }

        public void GetAllValuesAtOrBelow(List<Node<T>> results)
        {
            if (Value != null)
            {
                results.Add(this);
            }
            if (Children != null)
            {
                foreach(var child in Children)
                {
                    child.GetAllValuesAtOrBelow(results);
                }
            }
        }

        /// <summary>
        /// Looks for a child node that has a key segment that matches part of the prefix of a given key segment
        /// </summary>
        /// <param name="keySegment"></param>
        /// <param name="result">The matching child node</param>
        /// <returns>The number of matching bytes</returns>
        internal Node<T> FindMatchingChild(byte[] keySegment, int startingCharacter, out int bytesMatching)
        {
            if (Children != null)
            {
                foreach(var child in Children)
                {
                    var matchingBytes = GetMatchingBytes(keySegment, startingCharacter, child.KeySegment);

                    if (matchingBytes > 0)
                    {
                        bytesMatching = matchingBytes;
                        return child;
                    }
                }
            }
            bytesMatching = 0;
            return null;
        }

        private void RemoveChild(Node<T> child)
        {
            Children = Children.Where(x => x != child).ToArray();
        }

        public void RemoveValue()
        {
            Value = null;
            if (Children == null)
            {
                if (Parent!.Children!.Length == 1)
                {
                    Parent.Children = null;
                }
                else
                {
                    Parent.RemoveChild(this);
                }
            }
            else
            {
                TryMergeChild();
                var parent = Parent;
                while (parent != null)
                {
                    if (!parent.TryMergeChild())
                    {
                        break;
                    }
                    parent = parent.Parent;
                }
            }
        }

        public bool TryMergeChild()
        {
            if (Children != null && Children.Length == 1)
            {
                var child = Children[0];
                var mergedKeySegment = new byte[KeySegment.Length + child.KeySegment.Length];
                Array.Copy(KeySegment, mergedKeySegment, KeySegment.Length);
                Array.Copy(child.KeySegment, 0, mergedKeySegment, KeySegment.Length, child.KeySegment.Length);
                Value = child.Value;
                Children = child.Children;
                KeySegment = mergedKeySegment;
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetMatchingBytes(in ReadOnlySpan<byte> key, int keyStartingCharacter, in ReadOnlySpan<byte> keySegmentToMatch)
        {
            var keySpan = key.Slice(keyStartingCharacter);
            var bytesToCheck = Math.Min(keySpan.Length, keySegmentToMatch.Length);
            for (int i = 0; i < bytesToCheck; i++)
            {
                if (keySpan[i] != keySegmentToMatch[i])
                {
                    return i;
                }
            }

            return bytesToCheck;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetMatchingBytes(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> keySegmentToMatch)
        {            
            var bytesToCheck = Math.Min(key.Length, keySegmentToMatch.Length);
            for (int i = 0; i < bytesToCheck; i++)
            {
                if (key[i] != keySegmentToMatch[i])
                {
                    return i;
                }
            }
            return bytesToCheck;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] GetKeyBytes(string key)
        {
            return System.Text.Encoding.UTF8.GetBytes(key);
        }

        public int GetChildrenCount()
        {
            return GetChildrenCountInternal(0);
        }

        private int GetChildrenCountInternal(int runningCount)
        {
            if (Children != null)
            {
                foreach (var child in Children)
                {
                    runningCount++;
                    runningCount = child.GetChildrenCountInternal(runningCount);
                }
            }
            return runningCount;
        }

        public int GetValuesCount()
        {
            return GetValuesCountInternal(0);
        }

        private int GetValuesCountInternal(int runningCount)
        {
            if (Children != null)
            {
                foreach (var child in Children)
                {
                    if (child.Value != null)
                    {
                        runningCount++;
                    }
                    runningCount = child.GetChildrenCountInternal(runningCount);
                }
            }
            return runningCount;
        }

    }
}