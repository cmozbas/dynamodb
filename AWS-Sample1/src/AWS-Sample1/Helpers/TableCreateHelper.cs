using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AWS_Sample1.Helpers
{
    public class TableCreateHelper
    {
        private readonly AmazonDynamoDBClient client;

        public TableCreateHelper(AmazonDynamoDBClient client)
        {
            this.client = client;
        }

        public async Task<bool> CreateActorTableAsync()
        {
            var currentTables = await client.ListTablesAsync();
            List<string> listTable = currentTables.TableNames;
            string tableName = "Actors";
            bool tablesAdded = false;

            if (!listTable.Contains("Actors"))
            {
                await client.CreateTableAsync(new CreateTableRequest
                {
                    TableName = tableName,
                    ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 3, WriteCapacityUnits = 1 },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = "Name",
                            KeyType = KeyType.HASH
                        }
                    },
                    AttributeDefinitions = new List<AttributeDefinition>
                    {
                        new AttributeDefinition { AttributeName = "Name", AttributeType = ScalarAttributeType.S }
                    }
                });
                tablesAdded = true;
            }

            if (tablesAdded)
            {
                await WaitForTableCreation(tableName);
            }

            return tablesAdded;
        }

        /// <summary>
        /// Wait for table creation
        /// </summary>
        /// <param name="client"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private async Task<bool> WaitForTableCreation(string tableName)
        {
            bool allActive = false;
            do
            {
                allActive = true;
                Thread.Sleep(TimeSpan.FromSeconds(5));

                TableStatus tableStatus = await GetTableStatusAsync(tableName);
                if (!object.Equals(tableStatus, TableStatus.ACTIVE))
                    allActive = false;

            } while (!allActive);

            return allActive;
        }


        /// <summary>
        /// Retrieves a table status. Returns empty string if table does not exist.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private async Task<TableStatus> GetTableStatusAsync(string tableName)
        {
            try
            {
                var tableRes = await client.DescribeTableAsync(new DescribeTableRequest { TableName = tableName });
                var table = tableRes.Table;

                return (table == null) ? null : table.TableStatus;
            }
            catch (AmazonDynamoDBException db)
            {
                if (db.ErrorCode == "ResourceNotFoundException")
                    return string.Empty;
                throw;
            }
        }
    }
}
