using System.Collections.Generic;
using System.Threading.Tasks;

namespace DynamoDb.Fluent
{
    public interface IObjectQuery<T>
    {
        IQueryCondition<T> WithPrimaryKey();
        IQueryCondition<T> WithSecondaryKey();
        IScanCondition<T> WithFilter(string fieldName);
        Task<(T[] items, int Count)> Get(int limit);
        Task<T[]> Get();
        Task<int> Delete();
    }

    public interface ITable<T> : ITableIndex<T>
    {
        ITableIndex<T> WithIndex(string indexName);
        Task<T> Put(T item);
        Task<T> Delete(T item);
    }

    public interface ITableIndex<T>
    {
        IObjectQuery<T> Query();   
        Task<T> Find(object hashKey, object sortKey);
    }
}