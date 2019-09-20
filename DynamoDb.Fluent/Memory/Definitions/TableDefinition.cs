using System.Collections.Generic;

namespace DynamoDb.Fluent.Memory.Definitions
{
    public class TableDefinition
    {
        public string Name { get; set; }
        public AttributeDefinition HashKey { get; set; }
        public AttributeDefinition SortKey { get; set; }
        public List<IndexDefinition> Indexes { get; set; }
    }
}