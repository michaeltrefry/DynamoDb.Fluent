using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DynamoDb.Fluent
{
    public interface IObjectQuery<T> where T : class, new()
    {
        IObjectQuery<T> WithPrimaryKey(object keyValue);
        IQueryCondition<T> WithSecondaryKey();
        IScanCondition<T> WithFilter(string fieldName);
        Task<(T[] items, int count, string pageToken)> Get(int limit, string pageToken = null);
        IObjectQuery<T> Descending();
        Task<T[]> Get();
        Task<int> Delete();
        Task<int> Update(Action<T> updateAction);
    }
}