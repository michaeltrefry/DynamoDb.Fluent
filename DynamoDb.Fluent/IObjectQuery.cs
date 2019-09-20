using System.Collections.Generic;
using System.Threading.Tasks;

namespace DynamoDb.Fluent
{
    public interface IObjectQuery<T> where T : class, new()
    {
        IQueryCondition<T> WithPrimaryKey();
        IQueryCondition<T> WithSecondaryKey();
        IScanCondition<T> WithFilter(string fieldName);
        Task<(T[] items, int Count)> Get(int limit);
        IObjectQuery<T> Descending();
        Task<T[]> Get();
        Task<int> Delete();
    }
}