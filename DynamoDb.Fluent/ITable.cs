using System.Threading.Tasks;

namespace DynamoDb.Fluent
{
    public interface ITable<T> : ITableIndex<T> where T : class, new()
    {
        ITableIndex<T> WithIndex(string indexName);
        Task<T> Put(T item);
        Task<T[]> Put(T[] item);
        Task<T> Delete(T item);
        Task<T[]> Delete(T[] items);
    }
}