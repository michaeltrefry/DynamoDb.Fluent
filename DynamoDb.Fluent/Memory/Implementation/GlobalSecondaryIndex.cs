using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Collections;
using DynamoDb.Fluent.Memory.Definitions;
using Newtonsoft.Json.Linq;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class GlobalSecondaryIndex
    {
        private readonly ConcurrentDictionary<string, ConcurrentSortedDictionary<string, List<ItemPointer>>> data;
        private readonly IndexDefinition definition;
        private readonly string defaultSort = "default";
        public GlobalSecondaryIndex(IndexDefinition definition)
        {
            this.definition = definition;
            this.Name = definition.Name;
            data = new ConcurrentDictionary<string, ConcurrentSortedDictionary<string, List<ItemPointer>>>();
        }
        public string Name { get; }
        
        public void Index(JToken token, ItemPointer pointer)
        {
            var indexPointer = definition.GetPointer(token);
            if (indexPointer != null)
                Put(indexPointer.HashKey, indexPointer.SortKey, pointer);
        }

        public void Remove(JToken token, ItemPointer pointer)
        {
            var indexPointer = definition.GetPointer(token);
            if (indexPointer != null && data.TryGetValue(indexPointer.HashKey, out var sortData))
            {
                if (sortData.TryGetValue(indexPointer.SortKey ?? defaultSort, out var pointers))
                {
                    pointers.Remove(pointer);
                }
            }
        }

        public IEnumerable<ItemPointer> Query(QueryOperation operation)
        {
            if (operation.HashCondition == null) 
                return FullIndexScan();
            
            var sortData = FilterByHashCondition(operation.HashCondition);
            if (operation.SortCondition != null)
            {
                return FilterbySortCondition(sortData, operation.SortCondition);
            }
            return sortData.SelectMany(s => s.Values.SelectMany(l => l)).Distinct().ToArray();
        }
        
        private void Put(string hashKey, string sortKey, ItemPointer pointer)
        {
            var sortData = data.GetOrAdd(hashKey, s => new ConcurrentSortedDictionary<string, List<ItemPointer>>());
            var pointers = sortData.GetOrAdd(sortKey ?? defaultSort, s => new List<ItemPointer>());
            if (!pointers.Contains(pointer))
            {
                pointers.Add(pointer);
            }
        }

        private List<ConcurrentSortedDictionary<string, List<ItemPointer>>> FilterByHashCondition(Condition condition)
        {
            var results = new List<ConcurrentSortedDictionary<string, List<ItemPointer>>>();
            if (condition.Operator == ScanOperator.Equal)
            {
                if(data.TryGetValue(condition.Value, out var sortData))
                    results.Add(sortData);
            }
            else
            {
                results.AddRange(data.Where(h => condition.Passes(h.Key)).Select(h2 => h2.Value));
            }
            return results;
        }

        private ItemPointer[] FilterbySortCondition(List<ConcurrentSortedDictionary<string, List<ItemPointer>>> sortDataList, Condition condition)
        {
            var results = new List<ItemPointer>();
            foreach (var sortedData in sortDataList)
            {
                if (condition.Operator == ScanOperator.Equal)
                {
                    if(sortedData.TryGetValue(condition.Value, out var list))
                        results.AddRange(list);
                }
                else
                {
                    results.AddRange(sortedData.Where(h => condition.Passes(h.Key)).SelectMany(h2 => h2.Value));
                }
            }

            return results.Distinct().ToArray();
        }

        private ItemPointer[] FullIndexScan()
        {
            return data.SelectMany(d => d.Value.SelectMany(s => s.Value.Select(l => l))).Distinct().ToArray();
        }
        
    }
}