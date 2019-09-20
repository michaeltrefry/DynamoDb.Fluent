using System.Collections.Generic;

namespace DynamoDb.Fluent.Memory.Definitions
{
    public class IndexDefinition : TableDefinition
    {
        public IndexInclude Include { get; set; }
        public List<string> Attributes { get; set; }
    }
}