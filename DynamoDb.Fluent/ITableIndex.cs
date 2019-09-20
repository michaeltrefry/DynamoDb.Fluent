using System.Threading.Tasks;

namespace DynamoDb.Fluent
{
    public interface ITableIndex<T> where T : class, new()
    {
        IObjectQuery<T> Query();   
        Task<T> Find(object hashKey, object sortKey);
    }
}