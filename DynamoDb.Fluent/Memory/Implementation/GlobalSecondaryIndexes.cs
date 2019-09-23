using System.Collections.Generic;
using DynamoDb.Fluent.Memory.Collections;
using DynamoDb.Fluent.Memory.Definitions;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class GlobalSecondaryIndexes
    {
        private ConcurrentSortedDictionary<string, GlobalSecondaryIndex> indexes;

        public GlobalSecondaryIndexes(List<IndexDefinition> definitions)
        {
            indexes = new ConcurrentSortedDictionary<string, GlobalSecondaryIndex>();
            foreach (var indexDefinition in definitions)
            {
                indexes.TryAdd(indexDefinition.Name, new GlobalSecondaryIndex(indexDefinition));
            }
        } 
        
        public void Index(JToken token, ItemPointer pointer)
        {
            foreach (var index in indexes)
            {
                index.Value.Index(token, pointer);
            }
        }

        public void Remove(JToken token, ItemPointer pointer)
        {
            foreach (var index in indexes)
            {
                index.Value.Remove(token, pointer);
            }
        }
        
        public IEnumerable<ItemPointer> Query(QueryOperation query)
        {
            if (query.IndexName != null && indexes.TryGetValue(query.IndexName, out var index))
                return index.Query(query);
         
            return new ItemPointer[]{};
        }

    }
}