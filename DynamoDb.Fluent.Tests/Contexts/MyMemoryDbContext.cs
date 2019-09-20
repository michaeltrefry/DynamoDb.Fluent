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
                Name = "Users",
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
                        Name = "user-email-index",
                        HashKey = new AttributeDefinition()
                        {
                            Name = "Email",
                            Type = AttributeType.String
                        }
                    }
                }
            });
            CreateTable(new TableDefinition()
            {
                Name = "UserEntities",
                HashKey = new AttributeDefinition()
                {
                    Name = "UserId",
                    Type = AttributeType.String
                },
                SortKey = new AttributeDefinition()
                {
                    Name = "Id",
                    Type = AttributeType.String
                },
                Indexes = new List<IndexDefinition>()
                {
                    new IndexDefinition()
                    {
                        Name = "name-index",
                        HashKey = new AttributeDefinition()
                        {
                            Name = "Name",
                            Type = AttributeType.String
                        },
                        SortKey = new AttributeDefinition()
                        {
                            Name = "Id",
                            Type = AttributeType.String
                        }
                    }
                }
            });
        }
        public ITable<User> Users => this.GetTable<User>("Users");
        public ITable<UserEntity> UserEntities => this.GetTable<UserEntity>("UserEntities");
    }
}