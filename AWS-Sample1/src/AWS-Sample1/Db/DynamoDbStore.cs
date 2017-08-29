using System;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;

namespace AWS_Sample1.Db
{
    public class DynamoDbStore : IDocumentDbStore
    {
        private readonly DynamoDbStoreConfig config;

        public DynamoDbStore(DynamoDbStoreConfig config)
        {
            this.ValidateConfig(config);
            this.config = config;
        }

        public async Task<T> GetByIdAsync<T>(string tableName, dynamic key, dynamic secondaryKey = null)
        {
            var jsonModel = await this.GetByIdAsync(tableName, key, secondaryKey)
                .ConfigureAwait(false);

            var model = jsonModel != null 
                ? JsonConvert.DeserializeObject<T>(jsonModel) 
                : null;  

            return model;
        }

        public async Task<string> GetByIdAsync(string tableName, dynamic key, dynamic secondaryKey = null)
        {
            using (var client = this.CreateDbClient())
            {
                var table = Table.LoadTable(client, tableName);
                var document = await table.GetItemAsync(key, secondaryKey, 
                    new GetItemOperationConfig { ConsistentRead = this.config.UseConsistentRead })
                    .ConfigureAwait(false);

                return document?.ToJson();
            }
        }

        public async Task InsertOrUpdateAync<T>(string tableName, T model)
        {
            string jsonModel = model != null
                ? JsonConvert.SerializeObject(model)
                : null;

            await this.InsertOrUpdateAync(tableName, jsonModel)
                .ConfigureAwait(false);
        }

        public async Task InsertOrUpdateAync(string tableName, string jsonModel)
        {
            if (string.IsNullOrEmpty(jsonModel))
                throw new ArgumentNullException(nameof(jsonModel));

            using (var client = this.CreateDbClient())
            {
                var table = Table.LoadTable(client, tableName);
                var document = Document.FromJson(jsonModel);
                await table.PutItemAsync(document).ConfigureAwait(false);
            }
        }

        private AmazonDynamoDBClient CreateDbClient()
        {
            return new AmazonDynamoDBClient(
                this.config.AccessKey, this.config.SecretAccessKey,
                RegionEndpoint.GetBySystemName(this.config.RegionName));
        }

        private void ValidateConfig(DynamoDbStoreConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrEmpty(config.AccessKey))
                throw new ArgumentNullException(nameof(config.AccessKey));
            if (string.IsNullOrEmpty(config.SecretAccessKey))
                throw new ArgumentNullException(nameof(config.SecretAccessKey));
            if (string.IsNullOrEmpty(config.RegionName))
                throw new ArgumentNullException(nameof(config.RegionName));
        }
    }

    public class DynamoDbStoreConfig
    {
        public string AccessKey { get; set; }
        public string SecretAccessKey { get; set; }
        public string RegionName { get; set; }
        public bool UseConsistentRead { get; set; }
    }
}
