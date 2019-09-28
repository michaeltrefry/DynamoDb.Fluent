using System.Collections.Concurrent;
using System.Collections.Generic;
using DynamoDb.Fluent.Memory.Definitions;
using DynamoDb.Fluent.Memory.Implementation;

namespace DynamoDb.Fluent.Memory
{
    public class MemoryDbContext : IDynamoDbContext
    {
        private ConcurrentDictionary<string, MemoryTable> tables;

        public MemoryDbContext()
        {
            tables = new ConcurrentDictionary<string, MemoryTable>();
        }
        
        public void CreateTable(TableDefinition definition)
        {
            tables.TryAdd(definition.Name, new MemoryTable(definition));
        }
        
        public ITable<T> GetTable<T>(string tableName) where T : class, new()
        {
            if(!tables.TryGetValue(tableName, out var table))
                throw new KeyNotFoundException($"No table named {tableName} found.");
            
            return new MemoryDbTable<T>(table);
        }
    }
}