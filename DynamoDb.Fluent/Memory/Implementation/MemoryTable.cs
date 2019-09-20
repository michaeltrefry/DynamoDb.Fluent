using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Definitions;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class MemoryTable
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, JToken>> data;
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, ItemPointer>>> indexes;
        private readonly TableDefinition definition;
        public MemoryTable(TableDefinition definition)
        {
            this.definition = definition;
            data = new ConcurrentDictionary<string, ConcurrentDictionary<string, JToken>>();
            indexes = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, ItemPointer>>>();
        }
        public string Name => definition.Name;
        public TableDefinition Definition => definition;

        public T Put<T>(T item) where T : class, new()
        {
            var value = JToken.FromObject(item);
            var pointer = GetIndexPointer(value);

            var hashData = data.GetOrAdd(pointer.HashKey, s => new ConcurrentDictionary<string, JToken>());
            hashData.AddOrUpdate(pointer.SortKey, value, (s, token) => value);
            IndexItem(value, pointer);
            return item;
        }

        public T Get<T>(string hashKey, string sortKey)
        {
            if (!data.TryGetValue(hashKey, out var hashData))
                throw new KeyNotFoundException();
            if (!hashData.TryGetValue(sortKey, out var value))
                throw new KeyNotFoundException();

            return value.ToObject<T>();
        }

        private JToken GetToken(string hashKey, string sortKey)
        {
            if (!data.TryGetValue(hashKey, out var hashData))
                return null;
            return !hashData.TryGetValue(sortKey, out var value) ? null : value;
        }
        
        public T[] Get<T>(string hashKey)
        {
            if (!data.TryGetValue(hashKey, out var hashData))
                throw new KeyNotFoundException();
            var sortKey = definition.SortKey.Name;
            return hashData.Select(item => item.Value)
                .OrderBy(value => value[sortKey].Value<string>())
                .Select(v => v.ToObject<T>()).ToArray();
        }
        
        public T GetByIndex<T>(string indexName, string hashKey, string sortKey)
        {
            if (indexes == null)
                throw new KeyNotFoundException($"No index by the name {indexName}");
            
            if (!indexes.TryGetValue(indexName, out var indexData))
                throw new KeyNotFoundException($"No index by the name {indexName}");
            
            if (!indexData.TryGetValue(hashKey, out var hashData))
                throw new KeyNotFoundException();
            
            return !hashData.TryGetValue(sortKey, out var value) ? default : Get<T>(value.HashKey, value.SortKey);
        }

        public T[] GetByIndex<T>(string indexName, string hashKey)
        {
            if (indexes == null)
                throw new KeyNotFoundException($"No index by the name {indexName}");
            
            if (!indexes.TryGetValue(indexName, out var indexData))
                throw new KeyNotFoundException($"No index by the name {indexName}");
            
            if (!indexData.TryGetValue(hashKey, out var hashData))
                throw new KeyNotFoundException();

            return hashData.Select(h => h.Value)
                .OrderBy(p => p.SortKey)
                .Select(p2 => Get<T>(p2.HashKey, p2.SortKey))
                .ToArray();
        }

        public T Delete<T>(T item)
        {
            var value = JToken.FromObject(item);
            var pointer = GetIndexPointer(value);
            
            if (!data.TryGetValue(pointer.HashKey, out var hashData))
                throw new KeyNotFoundException();
            if (!hashData.TryRemove(pointer.SortKey, out value))
                throw new KeyNotFoundException();
            if (hashData.IsEmpty)
            {
                data.TryRemove(pointer.HashKey, out hashData);
            }

            return value.ToObject<T>();
        }

        public (T[], int) Query<T>(QueryOperation query)
        {
            var results = new List<JToken>();
            var sortKey = definition.SortKey.Name;
            
            if (query.IndexName != null)
            {
                if (indexes == null)
                    throw new KeyNotFoundException($"No index by the name {query.IndexName}");
                
                if (!indexes.TryGetValue(query.IndexName, out var indexData))
                    throw new KeyNotFoundException($"No index by the name {query.IndexName}");

                sortKey = definition.Indexes.Find(d => d.Name == query.IndexName).SortKey?.Name ?? definition.SortKey.Name;
                var pointers = GetValuesByKeyConditions(indexData, query.HashCondition, query.SortCondition);
                results.AddRange(pointers.Select(p=> GetToken(p.HashKey, p.SortKey)).Where(t=>t != null));
            }
            else
            {
                results.AddRange(GetValuesByKeyConditions<JToken>(data, query.HashCondition, query.SortCondition));    
            }
            
            if (query.Conditions != null && query.Conditions.Any())
            {
                var newResults = new List<JToken>();
                foreach (var result in results)
                {
                    foreach (var condition in query.Conditions)
                    {
                        var value = result[condition.FieldName].Value<string>();
                        if (IsConditionMet(value, condition))
                            newResults.Add(result);
                    }
                }

                results = newResults;
            }

            var count = results.Count;
            
            var orderedResults = results.OrderBy(t => t[sortKey].Value<string>()).ToArray();
            if (query.Descending)
                orderedResults = orderedResults.Reverse().ToArray();
            
            if (query.Limit > 0)
                return (orderedResults.Take(query.Limit)
                    .Select(t2 => t2.ToObject<T>()).ToArray(), count);
            
            return (orderedResults
                .Select(t2 => t2.ToObject<T>()).ToArray(), count);
            
        }


        private List<T> GetValuesByKeyConditions<T>(ConcurrentDictionary<string, ConcurrentDictionary<string, T>> data, Condition hashCondition, Condition sortCondition)
        {
            var results = new List<T>();
            if (hashCondition != null)
            {
                List<ConcurrentDictionary<string, T>> queryData = new List<ConcurrentDictionary<string, T>>();
                if (hashCondition.Operator == ScanOperator.Equal)
                {
                    if (data.TryGetValue(hashCondition.Value, out var hashData))
                        queryData.Add(hashData);
                }
                else
                {
                    queryData.AddRange(data.Where(h => IsConditionMet(h.Key, hashCondition)).Select(h2 => h2.Value));
                }

                if (sortCondition != null)
                {
                    foreach (var hashData in queryData)
                    {
                        if (sortCondition.Operator == ScanOperator.Equal)
                        {
                            if (hashData.TryGetValue(sortCondition.Value, out var value))
                                results.Add(value);
                        }
                        else
                        {
                            results.AddRange(hashData.Where(h => IsConditionMet(h.Key, sortCondition))
                                .Select(h2 => h2.Value));
                        }
                    }
                }
                else
                {
                    foreach (var qdata in queryData)
                    {
                        results.AddRange(qdata.Select(q=>q.Value));
                    }
                }
            } 
            else if (sortCondition != null)
            {
                foreach (var hashData in data.Values)
                {
                    if (sortCondition.Operator == ScanOperator.Equal)
                    {
                        if (hashData.TryGetValue(sortCondition.Value, out var value))
                            results.Add(value);
                    }
                    else
                    {
                        results.AddRange(hashData.Where(h => IsConditionMet(h.Key, sortCondition))
                            .Select(h2 => h2.Value));
                    }
                }
            }

            return results;
        }
        
        
        private void IndexItem(JToken value, ItemPointer pointer)
        {
            if (definition.Indexes == null)
                return;

            foreach (var index in definition.Indexes)
            {
                IndexItem(value, index, pointer);
            }
        }

        private void IndexItem(JToken value, IndexDefinition index, ItemPointer pointer)
        {
            var indexData = indexes.GetOrAdd(index.Name, s =>
            {
                var ix = new ConcurrentDictionary<string, ConcurrentDictionary<string, ItemPointer>>();
                indexes.TryAdd(index.Name, ix);
                return ix;
            });
            
            var hashKey =  value[index.HashKey.Name].Value<string>();
            var sortKey = value[index.SortKey?.Name ?? index.HashKey.Name].Value<string>();

            if (hashKey == null) 
                return;
            
            var hashData = indexData.GetOrAdd(hashKey, s => new ConcurrentDictionary<string, ItemPointer>());
            hashData.AddOrUpdate(sortKey, pointer, (s, p) => pointer);
        }

        private ItemPointer GetIndexPointer(JToken value)
        {
            var hashKey =  value[definition.HashKey.Name];
            var sortKey = value[definition.SortKey.Name];
            return new ItemPointer()
            {
                HashKey = hashKey.Value<string>(),
                SortKey = sortKey.Value<string>()
            };
        }

        private bool IsConditionMet(string value, Condition condition)
        {
            switch (condition.Operator)
            {
                case ScanOperator.Equal:
                    return value == condition.Value;
                case ScanOperator.NotEqual:
                    return value != condition.Value;
                case ScanOperator.BeginsWith:
                    return value.StartsWith(condition.Value);
                case ScanOperator.GreaterThan:
                    if (int.TryParse(value, out var val)
                        && int.TryParse(condition.Value, out var val1))
                        return val > val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) > 0;
                case ScanOperator.LessThan:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val < val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) < 0;
                case ScanOperator.GreaterThanOrEqual:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val >= val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) >= 0;
                case ScanOperator.LessThanOrEqual:
                    if (int.TryParse(value, out val)
                        && int.TryParse(condition.Value, out val1))
                        return val <= val1;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) <= 0;
                case ScanOperator.Between:
                    if (int.TryParse(value, out val)
                    && int.TryParse(condition.Value, out val1)
                    && int.TryParse(condition.Value2, out var val2))
                        return val > val1 && val < val2;
                    return string.Compare(value, condition.Value, StringComparison.InvariantCultureIgnoreCase) > 0
                           && string.Compare(value, condition.Value2, StringComparison.InvariantCultureIgnoreCase) < 0;
                case ScanOperator.In:
                    return condition.Values.Contains(value);
                case ScanOperator.IsNull:
                    return value == null;
                case ScanOperator.IsNotNull:
                    return value != null;
                case ScanOperator.Contains:
                    throw new NotImplementedException();
                case ScanOperator.NotContains:
                    throw new NotImplementedException();
            }

            return false;
        }
    }
}