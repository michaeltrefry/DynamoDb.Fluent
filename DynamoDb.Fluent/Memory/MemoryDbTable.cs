using System;
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

        public async Task<T> Find(object hashKey, object sortKey)
        {
            return await Task.Run(() => table.Get<T>(hashKey.ToString(), sortKey.ToString()));
        }

        public ITableIndex<T> WithIndex(string indexName)
        {
            return new MemoryDbTableIndex<T>(table, indexName);
        }

        public async Task<T> Put(T item) 
        {
            return await Task.Run(() => table.Put<T>(item));
        }

        public async Task<T[]> Put(T[] items)
        {
            foreach (var item in items)
            {
                await Put(item);
            }

            return items;
        }

        public async Task<T> Delete(T item)
        {
            return await Task.Run(() => table.Delete<T>(item));
        }

        public async Task<T[]> Delete(T[] items)
        {
            foreach (var item in items)
            {
                await Delete(item);
            }

            return items;
        }
    }
}