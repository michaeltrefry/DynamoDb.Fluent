using System.Collections.Generic;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class QueryOperation
    {
        public QueryOperation()
        {
            Conditions = new List<Condition>();
        }
        public Condition HashCondition { get; set; }
        public Condition SortCondition { get; set; }
        public string IndexName { get; set; }
        public List<Condition> Conditions { get; set; }
        public bool Descending { get; set; }
        public int Limit { get; set; }
    }
}