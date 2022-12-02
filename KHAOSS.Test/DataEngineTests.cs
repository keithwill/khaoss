using KHAOSS;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace KHAOSS.Test
{

    public class DataEngineTests : IClassFixture<DataEngineFixture>
    {
        // https://xunit.net/docs/shared-context
        private readonly DataEngineFixture fixture;

        private Store<Entity> store => fixture.Store;

        public DataEngineTests(DataEngineFixture fixture)
        {
            this.fixture = fixture;
            this.fixture.Engine.RemoveAllDocuments();
        }

        [Fact]
        public async Task CanCRUD()
        {
            var key = "crud";
            var entity = new TestDocument (key, 0, false, "crud body");
            var entitySaved = await store.Save(entity);
            var entitySavedExpected = entity with {Version = 1};
            
            Assert.Equal(entitySavedExpected, entitySaved);

            var entityRetreived = store.Get<TestDocument>(key);
            Assert.Equal(entitySavedExpected, entityRetreived);

            var entityToUpdate = entityRetreived with {Body = "crud body updated"}; 
            var expectedUpdatedEntity = entityToUpdate with {Version = 2};
            var entityUpdated = await store.Save(entityToUpdate);

            Assert.Equal(expectedUpdatedEntity, entityUpdated);

            var toDelete = entityUpdated with { Deleted = true };
            await store.Save(toDelete);
            var deletedDocument = store.Get<TestDocument>(key);
            Assert.Null(deletedDocument);
        }

        [Fact]
        public async Task TracksDeadSpace()
        {
            string key = "key";
            var entity1 = new TestDocument(key, 0, false, "some contents1");
            await store.Save(entity1);
            var entity2 = entity1 with { Body = "some contents2", Version = 1};
            await store.Save(entity2);
            var deadSpace = this.fixture.Engine.DeadEntityCount;
            var deadSpaceGreaterThanZero = deadSpace > 0;
            Assert.True(deadSpaceGreaterThanZero);
        }

        [Fact] 
        public async Task CanMaintainDeadSpace()
        {
            string key = "key";
            var entity1 = new TestDocument(key, 0, false, "some contents1");
            await store.Save(entity1);
            var entity2 = entity1 with { Body = "some contents2", Version = 1 };
            await store.Save(entity2);
            var deadSpace = this.fixture.Engine.DeadEntityCount;

            var deadSpaceBeforeMaintenance = this.fixture.Engine.DeadEntityCount;
            await this.fixture.Engine.ForceMaintenance();
            var deadSpaceAfterMaintenance = this.fixture.Engine.DeadEntityCount;
            var deadSpaceAfterMaintenanceIsLess = deadSpaceBeforeMaintenance > deadSpaceAfterMaintenance;
            Assert.True(deadSpaceAfterMaintenanceIsLess);
        }

        [Fact]
        public async Task PrefixSearch_PartialMatch()
        {
            string key = "prefix";
            string prefix = "pre";
            string body = "body";
            var expectedDocument = await store.Save(new TestDocument(key, 0, false, body));
            var prefixResults = store.GetByPrefix<TestDocument>(prefix, false);
            var firstMatchByPrefix = prefixResults.FirstOrDefault();
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task PrefixSearch_ExactMatch()
        {
            string key = "prefix";
            string prefix = "prefix";
            string body = "body";

            var expectedDocument = await store.Save(new TestDocument(key, 0, false, body));
            var prefixResults = store.GetByPrefix<TestDocument>(prefix, false);
            var firstMatchByPrefix = prefixResults.FirstOrDefault();
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task PrefixSearch_CanSortResults()
        {
            string key = "prefix";
            string prefix = "prefix";
            string body = "body";

            var expectedDocument = await store.Save(new TestDocument(key, 0, false, body));
            var prefixResults = store.GetByPrefix<TestDocument>(prefix, false);
            var firstMatchByPrefix = prefixResults.FirstOrDefault();
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task KeysAreCaseInsensitive()
        {
            var key = "crud";
            var notKey = "CrUd";


            await store.Save(new TestDocument(key, 0, false, "doc1"));
            await store.Save(new TestDocument(notKey, 0, false, "doc2"));

            var doc1 = store.Get<TestDocument>(key);
            var doc2 = store.Get<TestDocument>(notKey);

            Assert.NotEqual(doc1.Body, doc2.Body);
        }

        [Fact]
        public async Task Set_FailsWithOldVersionDocument()
        {
            var key = "crud";
            var version1 = await store.Save(new TestDocument(key, 0, false, "doc1"));
            var version0 = version1 with { Version = 0 };
            await Assert.ThrowsAsync<ConcurrencyException>(async () => await store.Save(version0));
        }

        [Fact]
        public async Task Delete_FailsWithOldVersionDocument()
        {
            var key = "crud";
            var version1 = await store.Save(new TestDocument(key, 0, false, "doc1"));
            var version0 = version1 with { Deleted = true, Version = 0 };
            await Assert.ThrowsAsync<ConcurrencyException>(async () => await store.Save(version0));
        }

        [Fact]
        public async Task Multi_CanProcessMultipleSetsAsTransaction()
        {

            var changes = new Entity[]
            {
                new TestDocument("1", 0, false, "body1"),
                new TestDocument("2", 0, false, "body2"),
                new Entity("3", 0, false)
            };

            var updated = await store.Save(changes);

            var expected = changes.Length;
            var actual = updated.Length;

            Assert.Equal(expected, actual);
        }

        /// <summary>
        /// Saves a document to the database, then performs a multi set that includes an update to the same document but with an
        /// older version number. Expected result is a failure
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task Multi_AllChangesRejectedOnAnyOldVersionDocument()
        {

            Entity[] changes = new Entity[]
            {
                new TestDocument("1", 0, false, "body1"),
                new TestDocument("2", 0, false, "body2"),
                new Entity("3", 0, false)
            };

            var updated = await store.Save(changes);

            updated[0] = updated[0] with { Version = 0 }; // Old version number

            await Assert.ThrowsAsync<ConcurrencyException>(async() => await store.Save(updated));
        }


    }
}
