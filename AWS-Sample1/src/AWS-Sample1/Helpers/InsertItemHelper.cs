using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using AWS_Sample1.Constants;
using Newtonsoft.Json;

namespace AWS_Sample1.Helpers
{
    public class InsertItemHelper
    {
        private readonly AmazonDynamoDBClient client;

        public InsertItemHelper(AmazonDynamoDBClient client)
        {
            this.client = client;
        }

        public async Task<bool> InsertItem(string tableName)
        {
            if (tableName == TableNameConstants.Actor)
            {
                Actor actor = new Actor
                {
                    Name = "Actor-" + Guid.NewGuid().ToString(),
                    Bio = "Christian Charles Philip Bale is an excellent horseman and an avid reader.",
                    BirthDate = new DateTime(1974, 1, 30),
                    HeightInMeters = 1.83f,
                    Weigth = 85.5f,
                    Address = new Address
                    {
                        City = "Los Angeles",
                        Country = "USA"
                    }
                };

                var actorJson = JsonConvert.SerializeObject(actor);
                var table = Table.LoadTable(client, TableNameConstants.Actor);
                var document = Document.FromJson(actorJson);
                await table.PutItemAsync(document).ConfigureAwait(false);
            }

            return true;
        }
    }
    
    public class Actor
    {
        public string Name { get; set; }

        public string Bio { get; set; }
        public DateTime BirthDate { get; set; }
        public float HeightInMeters { get; set; }
        public Address Address { get; set; }
        public string Comment { get; set; }
        public float Weigth { get; set; }

        public TimeSpan Age
        {
            get
            {
                return DateTime.UtcNow - BirthDate.ToUniversalTime();
            }
        }

        public override string ToString()
        {
            return string.Format("{0} - {1}", Name, BirthDate);
        }
    }

    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public string Country { get; set; }
    }
}
