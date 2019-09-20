using System;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent.Dynamo
{
    public class EntityConverter
    {
        private readonly DynamoDBContext context;
        private readonly DynamoDBOperationConfig operationConfig;
            
        public EntityConverter(AmazonDynamoDBClient client, string tableName)
        {
            context = new DynamoDBContext(client);
            operationConfig = new DynamoDBOperationConfig
            {
                OverrideTableName = tableName
            };
        }
            
        public Document ToDocument<T>(T item)
        {
            return context.ToDocument(item, operationConfig);
        }
            
        public T FromDocument<T>(Document document)
        {
            return context.FromDocument<T>(document, operationConfig);
        }

            
        public Primitive ToPrimative(object value)
        {
            if (value is string)
                return new Primitive(value.ToString());
            if (value is int)
                return new Primitive(value.ToString(), true);
            if (value is DateTime val)
                return new Primitive(val.ToString("O"));
            
            return null;
        }
    }
}