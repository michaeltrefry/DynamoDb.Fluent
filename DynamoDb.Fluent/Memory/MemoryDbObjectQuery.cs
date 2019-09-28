using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using DynamoDb.Fluent.Memory.Implementation;

namespace DynamoDb.Fluent.Memory
{
    public class MemoryDbObjectQuery<T> : IObjectQuery<T>, IScanCondition<T> where T : class, new()
    {
        private readonly MemoryTable table;
        private Condition currentCondition;
        private QueryOperation query;
        public MemoryDbObjectQuery(MemoryTable table)
        {
            this.table = table;
            query = new QueryOperation();
        }
        public MemoryDbObjectQuery(MemoryTable table, string indexName)
        {
            this.table = table;
            query = new QueryOperation {IndexName = indexName};
        }

        public IObjectQuery<T> WithPrimaryKey(object keyValue)
        {
            currentCondition = query.HashCondition = new Condition()
            {
                FieldName = table.Definition.HashKey.Name,
                Operator = ScanOperator.Equal,
                Value = keyValue.ToString()
            };
            return this;
        }

        public IQueryCondition<T> WithSecondaryKey()
        {
            currentCondition = query.SortCondition = new Condition()
            {
                FieldName = table.Definition.SortKey.Name
            };
            return this;
        }

        public IScanCondition<T> WithFilter(string fieldName)
        {
            currentCondition = new Condition()
            {
                FieldName = fieldName
            };
            query.Conditions.Add(currentCondition);
            return this;
        }

        public Task<(T[] items, int count, string pageToken)> Get(int limit, string pageToken = null)
        {
            query.Limit = limit;
            query.PageToken = pageToken;
            return Task.Run(() => table.Query<T>(query));
        }

        public IObjectQuery<T> Descending()
        {
            query.Descending = true;
            return this;
        }

        public Task<T[]> Get()
        {
            return Task.Run(() =>
            {
                var (items, _, _) = table.Query<T>(query);
                return items;
            });
        }

        public Task<int> Delete()
        {
            return Task.Run(() =>
            {
                var (items, count, _) = table.Query<T>(query);
                foreach (var item in items)
                {
                    table.Delete(item);
                }
                return items.Length;
            });
        }

        public Task<int> Update(Action<T> updateAction)
        {
            return Task.Run(() =>
            {
                var (items, count, _) = table.Query<T>(query);
                foreach (var item in items)
                {
                    updateAction(item);
                    table.Put(item);
                }
                return items.Length;
            });
        }

        public IObjectQuery<T> Equal(object value)
        {
            currentCondition.Operator = ScanOperator.Equal;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> LessThanOrEqual(object value)
        {
            currentCondition.Operator = ScanOperator.LessThanOrEqual;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> LessThan(object value)
        {
            currentCondition.Operator = ScanOperator.LessThan;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> GreaterThanOrEqual(object value)
        {
            currentCondition.Operator = ScanOperator.GreaterThanOrEqual;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> GreaterThan(object value)
        {
            currentCondition.Operator = ScanOperator.GreaterThan;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> BeginsWith(string value)
        {
            currentCondition.Operator = ScanOperator.BeginsWith;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> Between(object value1, object value2)
        {
            currentCondition.Operator = ScanOperator.Equal;
            currentCondition.Value = value1.ToString();
            currentCondition.Value2 = value2.ToString();
            return this;
        }

        public IObjectQuery<T> NotEqual(object value)
        {
            currentCondition.Operator = ScanOperator.NotEqual;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> IsNotNull()
        {
            currentCondition.Operator = ScanOperator.IsNotNull;
            return this;
        }

        public IObjectQuery<T> IsNull()
        {
            currentCondition.Operator = ScanOperator.IsNull;
            return this;
        }

        public IObjectQuery<T> Contains(object value)
        {
            currentCondition.Operator = ScanOperator.Contains;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> NotContains(object value)
        {
            currentCondition.Operator = ScanOperator.NotContains;
            currentCondition.Value = value.ToString();
            return this;
        }

        public IObjectQuery<T> In(IEnumerable<object> values)
        {
            currentCondition.Operator = ScanOperator.Equal;
            currentCondition.Values = values.Select(v => v.ToString()).ToArray();
            return this;
        }
    }
}