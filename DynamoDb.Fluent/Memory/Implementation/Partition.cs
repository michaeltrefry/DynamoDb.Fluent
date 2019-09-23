using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Collections;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class Partition : IEnumerable<KeyValuePair<string, JToken>>
    {
        private ConcurrentSortedDictionary<string, JToken> data;

        public Partition(string hashKey)
        {
            data = new ConcurrentSortedDictionary<string, JToken>();
            HashKey = hashKey;
        }
        
        public string HashKey { get; }
        public JToken Put(string sortKey, JToken item)
        {
            if (data.TryGetValue(sortKey, out var value))
            {
                return data.TryUpdate(sortKey, item) 
                    ? item 
                    : value;
            }
            
            return data.TryAdd(sortKey, item) 
                ? item 
                : null;
        }

        public JToken Get(string sortKey)
        {
            return data.TryGetValue(sortKey, out var value) 
                ? value 
                : null;
        }

        public JToken[] Get()
        {
            return data.Select(kv => kv.Value).ToArray();
        }
        
        public JToken Remove(string sortKey)
        {
            return data.TryRemove(sortKey, out var value) 
                ? value 
                : null;
        }

        public IEnumerable<JToken> Query(QueryOperation query)
        {
            var tokens = new List<JToken>();
            if (query.HashCondition != null && !query.HashCondition.Passes(HashKey))
                return new JToken[]{};
            if (query.SortCondition != null && query.SortCondition.Operator == ScanOperator.Equal)
                tokens.Add(Get(query.SortCondition.Value));
            else if (query.SortCondition != null)
                tokens.AddRange(data.Where(d => query.SortCondition.Passes(d.Key)).Select(d => d.Value));

            return query.FilterByConditions(tokens);
        }
        
        IEnumerator<KeyValuePair<string, JToken>> IEnumerable<KeyValuePair<string, JToken>>.GetEnumerator()
        {
            return data.GetEnumerator();
        }

        public IEnumerator GetEnumerator()
        {
            return data.GetEnumerator();
        }
    }
}