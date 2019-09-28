using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent.Memory.Implementation
{
    public class Condition
    {
        public string FieldName { get; set; }
        public ScanOperator Operator { get; set; }
        public string Value { get; set; }
        public string Value2 { get; set; }
        public string[] Values { get; set; }
        
    }
}