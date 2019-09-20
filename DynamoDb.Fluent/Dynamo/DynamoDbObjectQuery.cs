using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent.Dynamo
{
    internal class DynamoDbObjectQuery<T> : IObjectQuery<T>, IScanCondition<T> where T : class, new()
        {
            private readonly Table table;
            private readonly EntityConverter converter;
            private readonly string hashKeyName;
            private readonly string sortKeyName;
            private QueryOperationConfig queryOperation;
            private ScanOperationConfig scanOperation;
            private string currentAttributeName;
            private readonly string indexName;
            private List<string> indexAttributes;
            private bool isDescending;
            public DynamoDbObjectQuery(Table table, EntityConverter converter)
            {
                this.table = table;
                this.converter = converter;
                hashKeyName = table.Keys.SingleOrDefault(k => k.Value.IsHash).Key;
                sortKeyName = table.Keys.SingleOrDefault(k => !k.Value.IsHash).Key;
            }

            public DynamoDbObjectQuery(Table table, EntityConverter converter, string indexName)
            {
                this.table = table;
                this.converter = converter;
                var index = table.GlobalSecondaryIndexes[indexName];
                this.indexName = index.IndexName;
                hashKeyName = index.KeySchema.SingleOrDefault(k => k.KeyType == KeyType.HASH)?.AttributeName;
                sortKeyName =  index.KeySchema.SingleOrDefault(k => k.KeyType == KeyType.RANGE)?.AttributeName;
                if (index.Projection.ProjectionType == ProjectionType.INCLUDE)
                {
                    indexAttributes = index.Projection.NonKeyAttributes;
                }
            }

            private bool IsKeyField(string name)
            {
                return (name == hashKeyName || name == sortKeyName);
            }
            
            private void AddQueryFilterCondition(string name, QueryOperator operation, IEnumerable<object> values)
            {
                Primitive primitiveValue = null;
                DynamoDBEntry[] primitiveValues = null;
                if (values != null)
                {
                    var valuesArray = values.ToArray();
                    if (valuesArray.Length == 1)
                        primitiveValue = converter.ToPrimative(valuesArray.First());
                    else
                        primitiveValues = valuesArray.Select(v => (DynamoDBEntry)converter.ToPrimative(v)).ToArray();
                }
                
                if (queryOperation == null)
                {
                    if (!IsKeyField(name))
                        throw new ApplicationException("The first Filter must be a key field");
                    queryOperation = new QueryOperationConfig();
                    if (indexName != null)
                        queryOperation.IndexName = indexName;
                    if (primitiveValue != null)
                        queryOperation.Filter = new QueryFilter(name, operation, primitiveValue);
                    else if (primitiveValues != null)
                        queryOperation.Filter = new QueryFilter(name, operation, primitiveValues);
                }
                else
                {
                    if (primitiveValue != null)
                        queryOperation.Filter.AddCondition(name, operation, primitiveValue);
                    else if (primitiveValues != null)
                        queryOperation.Filter.AddCondition(name, operation, primitiveValues);
                }
            }
            
            private void AddScanFilterCondition(string name, ScanOperator operation, IEnumerable<object> values)
            {
                if (IsKeyField(name))
                    throw new ApplicationException("Use .WithPrimaryKey or .WithSecondaryKey to add conditions to key fields.");

                Primitive primitiveValue = null;
                DynamoDBEntry[] primitiveValues = null;
                if (values != null)
                {
                    var valuesArray = values.ToArray();
                    if (valuesArray.Length == 1)
                        primitiveValue = converter.ToPrimative(valuesArray.First());
                    else
                        primitiveValues = valuesArray.Select(v => (DynamoDBEntry)converter.ToPrimative(v)).ToArray();
                }
                
                if (queryOperation == null && scanOperation == null)
                {
                    scanOperation = new ScanOperationConfig();
                    if (indexName != null)
                        scanOperation.IndexName = indexName;
                    scanOperation.Filter = new ScanFilter();
                }

                if (queryOperation != null)
                {
                    if (primitiveValue != null)
                        queryOperation.Filter.AddCondition(name, operation, primitiveValue);
                    else if (primitiveValues != null)
                        queryOperation.Filter.AddCondition(name, operation, primitiveValues);
                    else 
                        queryOperation.Filter.AddCondition(name, operation, new DynamoDBEntry[]{});
                }
                else
                {
                    if (primitiveValue != null)
                        scanOperation.Filter.AddCondition(name, operation, primitiveValue);
                    else if (primitiveValues != null)
                        scanOperation.Filter.AddCondition(name, operation, primitiveValues);
                    else 
                        scanOperation.Filter.AddCondition(name, operation, new DynamoDBEntry[]{});
                }
            }
            
            public IQueryCondition<T> WithPrimaryKey()
            {
                currentAttributeName = hashKeyName;
                return this;
            }

            public IQueryCondition<T> WithSecondaryKey()
            {
                currentAttributeName = sortKeyName;
                return this;
            }

            IScanCondition<T> IObjectQuery<T>.WithFilter(string fieldName)
            {
                return WithFilter(fieldName);
            }

            public async Task<(T[] items, int Count)> Get(int limit)
            {
                if (queryOperation != null)
                {
                    return await GetQuery(queryOperation, limit);
                }

                if (scanOperation != null)
                {
                    return await GetScan(scanOperation, limit);
                }
                
                return await GetScan(new ScanOperationConfig(), limit);
            }

            public IObjectQuery<T> Descending()
            {
                isDescending = true;
                return this;
            }

            public async Task<T[]> Get()
            {
                var (items, _) = await Get(0);
                return items;
            }

            public async Task<int> Delete()
            {
               
                Search search;
                if (queryOperation != null)
                    search = table.Query(queryOperation);
                else if (scanOperation == null)
                    search = table.Scan(scanOperation);
                else
                    throw new ApplicationException("Cannot Delete entire table!");
                
                var batchWrite = table.CreateBatchWrite();
                
                var itemCount = 0;
                while (!search.IsDone)
                {
                    var batch = await search.GetNextSetAsync();
                    foreach (var document in batch)
                    {
                        batchWrite.AddItemToDelete(document);
                        itemCount++;
                    }
                }
                await batchWrite.ExecuteAsync();
                return itemCount;
            }

            public IScanCondition<T> WithFilter(string fieldName)
            {
                currentAttributeName = fieldName;
                return this;
            }

            private async Task<(T[] items, int Count)> GetQuery(QueryOperationConfig query, int limit)
            {
                query.BackwardSearch = isDescending;
                if (indexName != null)
                {
                    queryOperation.IndexName = indexName;
                    if (indexAttributes != null)
                    {
                        queryOperation.Select = SelectValues.SpecificAttributes;
                        queryOperation.AttributesToGet = indexAttributes;
                    }
                }
                var search = table.Query(query);
                var count = search.Count;
                
                IEnumerable<T> items = new T[0];
                var itemCount = 0;
                while (!search.IsDone)
                {
                    var batch = await search.GetNextSetAsync();
                    items = items.Concat(batch.Select(converter.FromDocument<T>));
                    itemCount += batch.Count;
                    if (limit > 0 && itemCount >= limit)
                        break;
                }
                var results = items.ToArray();
                if (limit > 0 && results.Length > limit)
                    return (results.Take(limit).ToArray(), count);
                return (results.ToArray(), count);
            }
            
            private async Task<(T[] items, int Count)> GetScan(ScanOperationConfig scan, int limit)
            {
                if (indexName != null)
                {
                    scanOperation.IndexName = indexName;
                    if (indexAttributes != null)
                    {
                        scanOperation.Select = SelectValues.SpecificAttributes;
                        scanOperation.AttributesToGet = indexAttributes;
                    }
                }
                var search = table.Scan(scan);
                    
                IEnumerable<T> items = new T[0];
                var itemCount = 0;
                while (!search.IsDone)
                {
                    var batch = await search.GetNextSetAsync();
                    items = items.Concat(batch.Select(converter.FromDocument<T>));
                    itemCount += batch.Count;
                    if (limit > 0 && itemCount >= limit && !isDescending)
                        break;
                }

                if (!isDescending) 
                    return (items.ToArray(), search.Count);
                
                //Scan operations don't support reverse order searches
                items = items.Reverse();
                if (limit > 0)
                    items = items.Take(limit);
                return (items.ToArray(), search.Count);

            }
            
            public IObjectQuery<T> Equal(object value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.Equal, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.Equal, new []{value});
                return this;
            }

            public IObjectQuery<T> LessThanOrEqual(object value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.LessThanOrEqual, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.LessThanOrEqual, new []{value});
                return this;
            }

            public IObjectQuery<T> LessThan(object value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.LessThan, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.LessThan, new []{value});
                return this;
            }

            public IObjectQuery<T> GreaterThanOrEqual(object value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.GreaterThanOrEqual, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.GreaterThanOrEqual, new []{value});
                return this;
            }

            public IObjectQuery<T> GreaterThan(object value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.GreaterThan, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.GreaterThan, new []{value});
                return this;
            }

            public IObjectQuery<T> BeginsWith(string value)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.BeginsWith, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.BeginsWith, new []{value});
                return this;
            }

            public IObjectQuery<T> Between(object value1, object value2)
            {
                if (IsKeyField(currentAttributeName))
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.Between, new []{value1, value2});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.Between, new []{value1, value2});
                return this;
            }

            public IObjectQuery<T> NotEqual(object value)
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.Equal, new []{value});
                return this;
            }

            public IObjectQuery<T> IsNotNull()
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.IsNotNull, null);
                return this;
            }

            public IObjectQuery<T> IsNull()
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.IsNull, null);
                return this;
            }

            public IObjectQuery<T> Contains(object value)
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.Contains, new[]{value});
                return this;
            }

            public IObjectQuery<T> NotContains(object value)
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.NotContains, new[]{value});
                return this;
            }

            public IObjectQuery<T> In(IEnumerable<object> values)
            {
                AddScanFilterCondition(currentAttributeName, ScanOperator.In, values);
                return this;
            }
        }
}