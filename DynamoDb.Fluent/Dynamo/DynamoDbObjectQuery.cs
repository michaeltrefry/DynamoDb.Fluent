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
            private bool currentAttributeIsKey;
            private readonly string indexName;
            private readonly List<string> indexAttributes;
            private bool isDescending;
            private readonly int maxBatchSize = 25;
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
                    {
                        queryOperation.IndexName = indexName;
                        if (indexAttributes != null)
                        {
                            queryOperation.Select = SelectValues.SpecificAttributes;
                            queryOperation.AttributesToGet = indexAttributes;
                        }
                    }

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
            
            public IObjectQuery<T> WithPrimaryKey(object key)
            {
                currentAttributeName = hashKeyName;
                currentAttributeIsKey = true;
                AddQueryFilterCondition(currentAttributeName, QueryOperator.Equal, new []{key});
                return this;
            }

            public IQueryCondition<T> WithSecondaryKey()
            {
                currentAttributeName = sortKeyName;
                currentAttributeIsKey = true;
                return this;
            }

            public IObjectQuery<T> Descending()
            {
                isDescending = true;
                return this;
            }

            public async Task<(T[] items, int count, string pageToken)> Get(int limit, string pageToken)
            {
                var (documents, count, token) = await GetDocuments(limit, pageToken);
                return (documents.Select(d => converter.FromDocument<T>(d)).ToArray(), count, token);
            }

            private async Task<(List<Document> items, int count, string token)> GetDocuments(int limit = 0, string pageToken = null)
            {
                if (queryOperation != null)
                {
                    return await GetQuery(queryOperation, limit, pageToken);
                }

                if (scanOperation != null)
                {
                    return await GetScan(scanOperation, limit, pageToken);
                }
                
                return await GetScan(new ScanOperationConfig(), limit, pageToken);
            }
            
            private async Task<(List<Document> items, int count, string token)> GetQuery(QueryOperationConfig query, int limit, string pageToken = null)
            {
                query.BackwardSearch = isDescending;
                if (limit > 0)
                {
                    query.Limit = limit;
                }
                if (pageToken != null)
                {
                    query.PaginationToken = pageToken;
                }
                var search = table.Query(query);
                var count = search.Count;

                var items = new List<Document>();
                while (!search.IsDone)
                {
                    var batch = await search.GetNextSetAsync();
                    items.AddRange(batch);
                    if (limit > 0 && items.Count >= limit)
                        break;
                }
                
                return (items.ToList(), count, search.PaginationToken);
            }
            
            private async Task<(List<Document> items, int count, string token)> GetScan(ScanOperationConfig scan, int limit = 0, string pageToken = null)
            {
                if (limit > 0)
                {
                    scan.Limit = limit;
                }
                if (pageToken != null)
                {
                    scan.PaginationToken = pageToken;
                }
                var search = table.Scan(scan);
                var count = search.Count;
                
                var items = new List<Document>();
                while (!search.IsDone)
                {
                    var batch = await search.GetNextSetAsync();
                    items.AddRange(batch);
                    if (limit > 0 && items.Count >= limit)
                        break;
                }

                if (isDescending)
                {
                    //Scan operations don't support reverse order searches
                    items.Reverse();
                }
                
                return (items, count, search.PaginationToken);
            }
            
            public async Task<T[]> Get()
            {
                var (items, _, _) = await Get(0, null);
                return items;
            }

            public async Task<int> Delete()
            {
                var total = 0;
                var (items, count, token) = await GetDocuments(maxBatchSize);
                while (items.Any())
                {
                    var batchWrite = table.CreateBatchWrite();
                    foreach (var item in items)
                    {
                        batchWrite.AddItemToDelete(item);
                    }
                    await batchWrite.ExecuteAsync();
                    total += items.Count;
                    if (items.Count < maxBatchSize)
                        break;
                    (items, count, token) = await GetDocuments(maxBatchSize, token);
                }
                return total;
            }

            public async Task<int> Update(Action<T> updateAction)
            {
                var total = 0;
                var (items, count, token) = await GetDocuments(maxBatchSize);
                while (items.Any())
                {
                    var batchWrite = table.CreateBatchWrite();
                    foreach (var item in items)
                    {
                        var value = converter.FromDocument<T>(item);
                        updateAction(value);
                        var updateItem = converter.ToDocument(value);
                        batchWrite.AddDocumentToPut(updateItem);
                    }
                    await batchWrite.ExecuteAsync();
                    total += items.Count;
                    if (items.Count < maxBatchSize)
                        break;
                    (items, count, token) = await GetDocuments(maxBatchSize, token);
                }
                return total;
            }
            
            public IScanCondition<T> WithFilter(string fieldName)
            {
                currentAttributeName = fieldName;
                currentAttributeIsKey = false;
                return this;
            }

            public IObjectQuery<T> Equal(object value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.Equal, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.Equal, new []{value});
                return this;
            }

            public IObjectQuery<T> LessThanOrEqual(object value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.LessThanOrEqual, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.LessThanOrEqual, new []{value});
                return this;
            }

            public IObjectQuery<T> LessThan(object value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.LessThan, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.LessThan, new []{value});
                return this;
            }

            public IObjectQuery<T> GreaterThanOrEqual(object value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.GreaterThanOrEqual, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.GreaterThanOrEqual, new []{value});
                return this;
            }

            public IObjectQuery<T> GreaterThan(object value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.GreaterThan, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.GreaterThan, new []{value});
                return this;
            }

            public IObjectQuery<T> BeginsWith(string value)
            {
                if (currentAttributeIsKey)
                    AddQueryFilterCondition(currentAttributeName, QueryOperator.BeginsWith, new []{value});
                else 
                    AddScanFilterCondition(currentAttributeName, ScanOperator.BeginsWith, new []{value});
                return this;
            }

            public IObjectQuery<T> Between(object value1, object value2)
            {
                if (currentAttributeIsKey)
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