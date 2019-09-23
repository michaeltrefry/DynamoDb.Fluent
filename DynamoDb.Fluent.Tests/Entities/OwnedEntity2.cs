using System;

namespace DynamoDb.Fluent.Tests.Entities
{
    public class OwnedEntity2
    {
        public OwnedEntity2()
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