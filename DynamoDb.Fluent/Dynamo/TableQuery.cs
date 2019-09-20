using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent.Dynamo
{
    internal class DynamoDbTable<T> : ITable<T> where T : class, new()
    {
        internal readonly EntityConverter Converter;
        internal readonly Table Table;
        internal DynamoDbTable(AmazonDynamoDBClient client, Table table)
        {
            this.Table = table;
            this.Converter = new EntityConverter(client, Table.TableName);
        }

        public IObjectQuery<T> Query()
        {
            return new DynamoDbObjectQuery<T>(Table, Converter);
        }

        public async Task<T> Find(object hashKey, object sortKey)
        {
            var key = Converter.ToPrimative(hashKey);
            var sort = Converter.ToPrimative(sortKey);
            var document = await Table.GetItemAsync(key, sort);
            return document == null ? default(T) : Converter.FromDocument<T>(document);
        }

        public ITableIndex<T> WithIndex(string indexName)
        {
            return new DynamoDbTableIndex<T>(this, indexName);
        }

        public async Task<T> Put(T item)
        {
            var document = Converter.ToDocument(item);
            document = await Table.PutItemAsync(document);
            return Converter.FromDocument<T>(document);
        }

        public async Task<T> Delete(T item)
        {
            var document = Converter.ToDocument(item);
            document = await Table.DeleteItemAsync(document);
            return Converter.FromDocument<T>(document);
        }
    }
}