using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DynamoDb.Fluent.Tests.Contexts;
using DynamoDb.Fluent.Tests.Entities;
using DynamoDb.Fluent.Tests.Utility;

namespace DynamoDb.Fluent.Tests
{
    [TestClass]
    public class MemoryDbTests
    {
        private static IMyDynamoContext context;
        private static string userId;
        [ClassInitialize]
        public static async Task Initialize(TestContext testContext)
        {
            context = new MyMemoryDbContext();
            for (var i = 0; i < 50; i++)
            {
                var item = await context.Users.Put(new User()
                {
                    Id = ShortGuid.NewId(),
                    Type = "User",
                    Name = $"TestUser{i}",
                    Email = $"user{i}@user.com",
                    PasswordHash = $"{((i*Math.PI)%i).GetHashCode().ToString()}",
                });
                for (var e = 0; e < 1000; e++)
                {
                    var entity = await context.UserEntities.Put(new UserEntity()
                    {
                        UserId = item.Id,
                        Id = ShortGuid.NewId(),
                        Created = DateTime.UtcNow,
                        Name = $"UserEntity{e}"
                    });
                }
                if (i == 34)
                    userId = item.Id;
            }
        }
        
        [TestMethod]
        public async Task TestFindUser()
        {
            var item = await context.Users.Find(userId,"User");
            Assert.IsNotNull(item);
        }
        
        
        [TestMethod]
        public async Task TestQuery()
        {
            var (items, count) = await context.Users.Query().WithPrimaryKey().Equal(userId).WithSecondaryKey().Equal("User")
                .WithFilter("Email").BeginsWith("user").Get(1);
            
            Assert.AreEqual(count, 1);
        }

        [TestMethod]
        public async Task TestIndex()
        {
            var result = await context.UserEntities.WithIndex("name-index").Query().WithPrimaryKey()
                .Equal("UserEntity98").Get();
            
            Assert.AreEqual(result.Length, 50);
        }

        [TestMethod]
        public async Task TestPutDelete()
        {
            var user = await context.Users.Put(new User()
            {
                Id = ShortGuid.NewId(),
                Type = "User",
                Name = $"User1",
                Email = $"user1@test.com",
                PasswordHash = "kdsjfihsd09fy9y4rhiosf",
            });

            user = await context.Users.Find(user.Id, "User");

            Assert.IsNotNull(user);
            
            await context.Users.Delete(user);

            KeyNotFoundException expected = null;
            try
            {
                user = await context.Users.Find(user.Id, "User");
            }
            catch (KeyNotFoundException ex)
            {
                expected = ex;
            }

            Assert.IsNotNull(expected);
        }
    }
}