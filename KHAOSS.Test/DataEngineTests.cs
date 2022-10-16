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
        private readonly Encoding utf8 = System.Text.Encoding.UTF8;

        public DataEngineTests(DataEngineFixture fixture)
        {
            this.fixture = fixture;
            this.fixture.Engine.RemoveAllDocuments();
        }

        private async Task< (Document Doc, TransactionResult Result) > AddDocument(string key, string body, int version)
        {
            var document = new Document ( version, utf8.GetBytes(body) );
            var result = await fixture.Store.Set(key, document);
            return (document, result);
        }

        [Fact]
        public async Task CanCRUD()
        {
            var key = "crud";

            var document = (await AddDocument(key, "crud body", 0)).Doc;
            var documentRetreived = await fixture.Store.Get(key);
            Assert.Equal(document.Body, documentRetreived.Body);

            var newDocument = (await AddDocument(key, "crud body updated", documentRetreived.Version)).Doc;
            var newDocumentRetrieved = await fixture.Store.Get(key);
            Assert.Equal(newDocument.Body, newDocumentRetrieved.Body);
            Assert.Equal(2, newDocumentRetrieved.Version);

            await fixture.Store.Remove(key, newDocumentRetrieved.Version);
            var deletedDocument = await fixture.Store.Get(key);
            Assert.Null(deletedDocument);
        }

        [Fact]
        public async Task TracksDeadSpace()
        {
            string key = "key";
            await AddDocument(key, "Contents1", 0);
            await AddDocument(key, "Contents2", 1);
            var deadSpace = this.fixture.Engine.DeadSpace;
            var deadSpaceGreaterThanZero = deadSpace > 0;
            Assert.True(deadSpaceGreaterThanZero);
        }

        [Fact]
        public async Task CanMaintainDeadSpace()
        {
            string key = "key";
            await AddDocument(key, "Contents1", 0);
            await AddDocument(key, "Contents2", 1);
            var deadSpaceBeforeMaintenance = this.fixture.Engine.DeadSpace;
            await this.fixture.Engine.ForceMaintenance();
            var deadSpaceAfterMaintenance = this.fixture.Engine.DeadSpace;
            var deadSpaceAfterMaintenanceIsLess = deadSpaceBeforeMaintenance > deadSpaceAfterMaintenance;
            Assert.True(deadSpaceAfterMaintenanceIsLess);
        }

        [Fact]
        public async Task PrefixSearch_PartialMatch()
        {
            string key = "prefix";
            string prefix = "pre";
            string body = "body";

            var expectedDocument = (await AddDocument(key, body, 0)).Doc;
            var prefixResults = await fixture.Store.GetByPrefix(prefix, false);
            var firstMatchByPrefix = prefixResults.FirstOrDefault().Value;
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task PrefixSearch_ExactMatch()
        {
            string key = "prefix";
            string prefix = "prefix";
            string body = "body";

            var expectedDocument = (await AddDocument(key, body, 0)).Doc;
            var prefixResults = await fixture.Store.GetByPrefix(prefix, false);
            var firstMatchByPrefix = prefixResults.FirstOrDefault().Value;
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task PrefixSearch_CanSortResults()
        {
            string key = "prefix";
            string prefix = "prefix";
            string body = "body";

            var expectedDocument = (await AddDocument(key, body, 0)).Doc;
            var prefixResults = await fixture.Store.GetByPrefix(prefix, true);
            var firstMatchByPrefix = prefixResults.FirstOrDefault().Value;
            Assert.Equal(expectedDocument, firstMatchByPrefix);
        }

        [Fact]
        public async Task KeysAreCaseInsensitive()
        {
            var key = "crud";
            var notKey = "CrUd";

            var document = (await AddDocument(key, "crud body", 0)).Doc;
            var documentFromMixedCaseKey = await fixture.Store.Get(notKey);

            Assert.NotEqual(document, documentFromMixedCaseKey);
        }

        [Fact]
        public async Task Set_FailsWithOldVersionDocument()
        {
            var key = "crud";
            var document = (await AddDocument(key, "crud body", 0)).Doc;
            
            var addUpdatedDoc = await AddDocument(key, "crud body new", 0);

            Assert.Equal(TransactionResult.FailedConcurrencyCheck, addUpdatedDoc.Result);
           
        }

        [Fact]
        public async Task Delete_FailsWithOldVersionDocument()
        {
            var key = "crud";
            var document = (await AddDocument(key, "crud body", 0)).Doc;

            var removeResult = await fixture.Store.Remove(key, 0);

            Assert.Equal(TransactionResult.FailedConcurrencyCheck, removeResult);
        }

        [Fact]
        public async Task Multi_CanProcessMultipleSetsAsTransaction()
        {
            var key1 = "crud1";
            var key2 = "crud2";

            var document1 = new Document ( 0, utf8.GetBytes("body1") );
            var document2 = new Document ( 0, utf8.GetBytes("body2") );

            var changes = new DocumentChange[]
            {
                new DocumentChange { Key = key1, ChangeType = DocumentChangeType.Set, Document = document1 },
                new DocumentChange { Key = key2, ChangeType = DocumentChangeType.Set, Document = document2 }
            };

            var actualTransactionResult = await fixture.Store.Multi(changes);
            var expectedTransactionResult = TransactionResult.Complete;

            Assert.Equal(expectedTransactionResult, actualTransactionResult);
        }

        /// <summary>
        /// Saves a document to the database, then performs a multi set that includes an update to the same document but with an
        /// older version number. Expected result is a TransactionResult.FailedConcurrencyCheck
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task Multi_AllChangesRejectedOnAnyOldVersionDocument()
        {

            var key1 = "crud1";
            var key2 = "crud2";

            await AddDocument(key1, "body1", 0); // Saved as version 1 in db

            var document1_version0 = new Document (0, utf8.GetBytes("body1") );
            var document2 =          new Document (0, utf8.GetBytes("body2") );

            var changes = new DocumentChange[]
            {
                new DocumentChange { Key = key1, ChangeType = DocumentChangeType.Set, Document = document1_version0 },
                new DocumentChange { Key = key2, ChangeType = DocumentChangeType.Set, Document = document2 }
            };

            var actualTransactionResult = await fixture.Store.Multi(changes);
            var expectedTransactionResult = TransactionResult.FailedConcurrencyCheck;

            Assert.Equal(expectedTransactionResult, actualTransactionResult);
        }


    }
}
