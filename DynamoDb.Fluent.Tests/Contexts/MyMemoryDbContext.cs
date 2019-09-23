using System.Collections.Generic;
using DynamoDb.Fluent.Memory;
using DynamoDb.Fluent.Memory.Definitions;
using DynamoDb.Fluent.Tests.Entities;

namespace DynamoDb.Fluent.Tests.Contexts
{
    public class MyMemoryDbContext : MemoryDbContext, IMyDynamoContext
    {
        public MyMemoryDbContext()
        {
            CreateTable(new TableDefinition()
            {
                Name = "UnitTests",
                HashKey = new AttributeDefinition()
                {
                    Name = "Id",
                    Type = AttributeType.String
                },
                SortKey = new AttributeDefinition()
                {
                    Name = "Type",
                    Type = AttributeType.String
                },
                Indexes = new List<IndexDefinition>()
                {
                    new IndexDefinition()
                    {
                        Name = "User-Email-index",
                        HashKey = new AttributeDefinition()
                        {
                            Name = "Email",
                            Type = AttributeType.String
                        }
                    },
                    new IndexDefinition()
                    {
                        Name = "OwnerId-Type-Index",
                        HashKey = new AttributeDefinition()
                        {
                            Name = "OwnerId",
                            Type = AttributeType.String
                        },
                        SortKey = new AttributeDefinition()
                        {
                            Name = "Type",
                            Type = AttributeType.String
                        }
                    }
                }
            });
        }
        public ITable<User> Users => this.GetTable<User>("UnitTests");
        public ITable<OwnedEntity> OwnedEntities => this.GetTable<OwnedEntity>("UnitTests");
        public ITable<T> Entities<T>() where T : class, new()
        {
            return this.GetTable<T>("UnitTests");
        }
    }
}