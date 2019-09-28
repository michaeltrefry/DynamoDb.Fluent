using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DynamoDb.Fluent.Tests.Contexts;
using DynamoDb.Fluent.Tests.Entities;
using DynamoDb.Fluent.Tests.Utility;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DynamoDb.Fluent.Tests
{
    [TestClass]
    public class DynamooDbTests
    {
        private static IMyDynamoContext context;
        
        [ClassInitialize]
        public static async Task Initialize(TestContext testContext)
        {
            Environment.SetEnvironmentVariable("AWS_ENABLE_ENDPOINT_DISCOVERY", "false");
            context = new MyDynamoContext();
        
        }

        
        
        [TestMethod]
        public async Task TestQuery()
        {
            var user = await context.Users.Put(new User()
            {
                Id = ShortGuid.NewId(),
                Type = "User",
                Name = $"User1",
                Email = $"user1@test.com",
                PasswordHash = "kdsjfihsd09fy9y4rhiosf",
            });
            
            var (items, count, pageToken) = await context.Users.Query().WithPrimaryKey(user.Id).WithSecondaryKey().Equal("User")
                .WithFilter("Email").BeginsWith("user").Get(1);
            
            await context.Users.Delete(user);
            
            Assert.AreEqual(count, 1);
        }

        [TestMethod]
        public async Task TestIndex()
        {
            var user = await context.Users.Put(new User()
            {
                Id = ShortGuid.NewId(),
                Name = $"User2",
                Email = $"user2@test.com",
                PasswordHash = "kdsjfihsd09fy9y4rhiosf",
            });

            var entity = await context.OwnedEntities.Put(new OwnedEntity()
            {
                OwnerId = user.Id,
                Id = ShortGuid.NewId(),
                Created = DateTime.UtcNow,
                Name = "OwnedEntity2"
            });
            
            var result = await context.OwnedEntities.WithIndex("OwnerId-Type-Index").Query()
                .WithPrimaryKey(user.Id).WithSecondaryKey().Equal("OwnedEntity") .Get();
            
            Assert.AreEqual(result.Length, 1);

            await context.Users.Delete(user);
            await context.OwnedEntities.Delete(entity);
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
            
            user = await context.Users.Find(user.Id, "User");
            
            Assert.IsNull(user);
        }

//        [TestMethod]
//        public async Task TestDeleteAll()
//        {
//            var users = await context.Users.Query().WithFilter("Type").Equal("User").Get();
//            await context.Users.Delete(users);
//
//            var entities = await context.OwnedEntities.Query().WithFilter("Type").Equal("OwnedEntity").Get();
//            await context.OwnedEntities.Delete(entities);
//            
//
//            var entity2s = await context.Entities<OwnedEntity2>().Query().WithFilter("Type").Equal("OwnedEntity2").Get();
//            await context.Entities<OwnedEntity2>().Delete(entity2s);
//            
//        }
    }
}