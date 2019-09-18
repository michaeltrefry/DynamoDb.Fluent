using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace DynamoDb.Fluent
{
    internal class DynamoDbTable<T> : ITable<T>
    {
        internal readonly EntityConverter Converter;
        internal readonly Table Table;
        internal DynamoDbTable(AmazonDynamoDBClient client, string tableName)
        {
            this.Table = Table.LoadTable(client, tableName);
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

    internal class DynamoDbTableIndex<T> : ITableIndex<T>
    {
        private readonly string indexName;
        private readonly DynamoDbTable<T> table;
        public DynamoDbTableIndex(DynamoDbTable<T> table, string indexName)
        {
            this.table = table;
            this.indexName = indexName;
        }

        public IObjectQuery<T> Query()
        {
            return new DynamoDbObjectQuery<T>(table.Table, table.Converter, indexName);
        }

        public async Task<T> Find(object indexHashKey, object indexSortKey)
        {
            var results = await Query().WithPrimaryKey().Equal(indexHashKey).WithSecondaryKey().Equal(indexSortKey).Get();
            return results.SingleOrDefault();
        }
    }
}