using System;

namespace DynamoDb.Fluent.Tests.Entities
{
    public class UserEntity
    {
        public string UserId { get; set; }
        public string Id  { get; set; }
        public string Name { get; set; }
        public DateTime Created { get; set; }
    }
}