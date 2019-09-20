using System.Threading.Tasks;
using DynamoDb.Fluent.Memory.Implementation;

namespace DynamoDb.Fluent.Memory
{
    public class MemoryDbTableIndex<T> : ITableIndex<T> where T : class, new()
    {
        private readonly MemoryTable table;
        private readonly string indexName;
        public MemoryDbTableIndex(MemoryTable table, string indexName)
        {
            this.table = table;
            this.indexName = indexName;
        }
        
        public IObjectQuery<T> Query()
        {
            return new MemoryDbObjectQuery<T>(table, indexName);
        }

        public Task<T> Find(object hashKey, object sortKey)
        {
            return Task.Run(() => table.Get<T>(hashKey.ToString(), sortKey.ToString()));
        }
    }
}