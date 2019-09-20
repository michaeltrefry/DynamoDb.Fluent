using System.Linq;
using System.Threading.Tasks;

namespace DynamoDb.Fluent.Dynamo
{
    internal class DynamoDbTableIndex<T> : ITableIndex<T> where T : class, new()
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