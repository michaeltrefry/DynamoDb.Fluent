using System;

namespace DynamoDb.Fluent.Tests.Entities
{
    public class User
    {
        public User()
        {
            Type = this.GetType().Name;
        }
        public string Id { get; set; }
        public string Type { get; set; }
        public int AdminStatus { get; set; }
        public DateTime AdminStatusDate { get; set; }
        public DateTime Created { get; set; }
        public string Name { get; set; }
        public DateTime Updated { get; set; }
        public int Visibility { get; set; }
        public DateTime? BirthDate { get; set; }
        public string Email { get; set; }
        public string PasswordHash { get; set; }
        public int UserType { get; set; }
    }
}