using System;

namespace DynamoDb.Fluent.Tests.Entities
{
    public class OwnedEntity
    {
        public OwnedEntity()
        {
            Type = this.GetType().Name;
        }
        public string OwnerId { get; set; }
        public string Type { get; set; }
        public string Id  { get; set; }
        public string Name { get; set; }
        public DateTime Created { get; set; }
    }
}