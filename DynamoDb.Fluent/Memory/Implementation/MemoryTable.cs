using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Collections;
using DynamoDb.Fluent.Memory.Definitions;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class MemoryTable
    {
        private PartitionCollection data;
        private GlobalSecondaryIndexes indexes;
        private readonly TableDefinition definition;
        public MemoryTable(TableDefinition definition)
        {
            this.definition = definition;
            data = new PartitionCollection();
            indexes = new GlobalSecondaryIndexes(definition.Indexes);
        }
        
        public string Name => definition.Name;
        public TableDefinition Definition => definition;

        public T Put<T>(T item) where T : class, new()
        {
            var token = JToken.FromObject(item);
            var pointer = definition.GetPointer(token);
            if (pointer == null)
            {
                throw new InvalidOperationException("Item does not contain required keys.'");
            }
            data.Put(pointer.HashKey, pointer.SortKey, token);
            indexes.Index(token, pointer);
            return item;
        }

        
        public T Get<T>(string hashKey, string sortKey) where T : class, new()
        {
            var token = data.Get(hashKey, sortKey);
            return token?.ToObject<T>();
        }

        public T[] Get<T>(string hashKey) where T : class, new()
        {
            var sortKey = definition.SortKey.Name;
            var items = data.Get(hashKey);
            
            return items
                .Select(v => v.ToObject<T>()).ToArray();
        }
        
        public T[] GetByIndex<T>(string indexName, string hashKey, string sortKey = null) where T : class, new()
        {
            var query = new QueryOperation()
            {
                IndexName = indexName,
                HashCondition = new Condition()
                {
                    Operator = ScanOperator.Equal,
                    Value = hashKey
                }
            };
            if (sortKey != null)
            {
                query.SortCondition = new Condition()
                {
                    Operator = ScanOperator.Equal,
                    Value = sortKey
                };
            }
            var pointers = indexes.Query(query);
            return pointers.Select(p => Get<T>(p.HashKey, p.SortKey)).ToArray();
        }

        public T Delete<T>(T item) where T : class, new()
        {
            var token = JToken.FromObject(item);
            var pointer = definition.GetPointer(token);
            if (pointer == null)
                return null;
            token = data.Remove(pointer.HashKey, pointer.SortKey);
            if (token != null) indexes.Remove(token, pointer);
            return token?.ToObject<T>();
        }

        public (T[], int) Query<T>(QueryOperation query) where T : class, new()
        {
            IEnumerable<JToken> tokens;
            var sortKey = definition.SortKey.Name;
            if (query.IndexName != null)
            {
                var indexResults = indexes.Query(query);
                tokens = indexResults.Select(p => data.Get(p.HashKey, p.SortKey));
                tokens = query.FilterByConditions(tokens);
            }
            else
            {
                tokens = data.Query(query);
            }
            
            tokens = query.Descending 
                ? tokens.OrderByDescending(t => t[sortKey].Value<string>()) 
                : tokens.OrderBy(t => t[sortKey].Value<string>());
            
            var results = tokens.ToArray();
            var count = results.Length;

            if (query.Limit > 0)
                results = results.Take(query.Limit).ToArray();
            
            return (results.Select(t2 => t2.ToObject<T>()).ToArray(), count);

        }
    }
}