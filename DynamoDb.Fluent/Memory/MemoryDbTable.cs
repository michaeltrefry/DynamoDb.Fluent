using System.Threading.Tasks;
using DynamoDb.Fluent.Memory.Implementation;

namespace DynamoDb.Fluent.Memory
{
    public class MemoryDbTable<T> : ITable<T> where T : class, new()
    {
        private readonly MemoryTable table;
        public MemoryDbTable(MemoryTable table)
        {
            this.table = table;
        }
        
        public IObjectQuery<T> Query()
        {
            return new MemoryDbObjectQuery<T>(table);
        }

        public Task<T> Find(object hashKey, object sortKey)
        {
            return Task.Run(() => table.Get<T>(hashKey.ToString(), sortKey.ToString()));
        }

        public ITableIndex<T> WithIndex(string indexName)
        {
            return new MemoryDbTableIndex<T>(table, indexName);
        }

        public Task<T> Put(T item) 
        {
            return Task.Run(() => table.Put<T>(item));
        }

        public Task<T> Delete(T item)
        {
            return Task.Run(() => table.Delete<T>(item));
        }
    }
}