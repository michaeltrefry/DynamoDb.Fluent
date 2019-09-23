using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class PartitionCollection : IEnumerable<KeyValuePair<string, Partition>>
    {
        private ConcurrentDictionary<string, Partition> data;

        public PartitionCollection()
        {
            data = new ConcurrentDictionary<string, Partition>();
        }
        public JToken Put(string hashKey, string sortKey, JToken item)
        {
            var partition = data.GetOrAdd(hashKey, s => new Partition(hashKey));
            return partition.Put(sortKey, item);
        }

        public IEnumerable<JToken> Query(QueryOperation query)
        {
            if (query.HashCondition == null) 
                return data.SelectMany(p => p.Value.Query(query));
            
            if (query.HashCondition.Operator == ScanOperator.Equal)
            {
                return data.TryGetValue(query.HashCondition.Value, out var partition) 
                    ? partition.Query(query) 
                    : new JToken[] { };
            }
            return data.Where(p => query.HashCondition.Passes(p.Key)).SelectMany(p => p.Value.Query(query));
        
        }
        
        public JToken Get(string hashKey, string sortKey)
        {
            return data.TryGetValue(hashKey, out var partition) 
                ? partition.Get(sortKey) 
                : null;
        }

        public JToken[] Get(string hashKey)
        {
            return data.TryGetValue(hashKey, out var partition) 
                ? partition.Get() 
                : new JToken[]{};
        }
        
        public JToken Remove(string hashKey, string sortKey)
        {
            return data.TryRemove(hashKey, out var partition) 
                ? partition.Remove(sortKey) 
                : null;
        }

        public Partition GetSortCollection(string hashKey)
        {
            return data.TryGetValue(hashKey, out var partition) 
                ? partition 
                : null;
        }
        
        public IEnumerator<KeyValuePair<string, Partition>> GetEnumerator()
        {
            return data.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return data.GetEnumerator();
        }
    }
}