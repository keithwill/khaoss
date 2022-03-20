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
            var document = new Document { Version = version, Body = utf8.GetBytes(body) };
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

        //[Fact]
        //public async Task Multi_CanProcessMultipleGetsAndSetsAsTransaction()
        //{
        //    throw new NotImplementedException();
        //}

        //[Fact]
        //public async Task Multi_AllChangesRejectedOnAnyOldVersionDocument()
        //{
        //    throw new NotImplementedException();
        //}


    }
}
