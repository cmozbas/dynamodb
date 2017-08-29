using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using AWS_Sample1.Models;
using AWS_Sample1.Helpers;
using AWS_Sample1.Constants;

namespace AWS_Sample1.Controllers
{
    public class DynamoController : Controller
    {
        private readonly AmazonDynamoDBClient amazonDynamoDBClient;

        public DynamoController(AmazonDynamoDBClient amazonDynamoDBClient)
        {
            this.amazonDynamoDBClient = amazonDynamoDBClient;
        }

        public IActionResult Index()
        {
            return View();
        }

        public async Task<IActionResult> ListTable()
        {
            List<TableModel> tables = new List<TableModel>();

            // Initial value for the first page of table names.
            string lastEvaluatedTableName = null;
            do
            {
                // Create a request object to specify optional parameters.
                var request = new ListTablesRequest
                {
                    Limit = 10, // Page size.
                    ExclusiveStartTableName = lastEvaluatedTableName
                };

                var response = await this.amazonDynamoDBClient.ListTablesAsync(request);

                foreach (string name in response.TableNames)
                {
                    tables.Add(new TableModel() { TableName = name });
                }

                lastEvaluatedTableName = response.LastEvaluatedTableName;

            } while (lastEvaluatedTableName != null);


            return View(tables);
        }

        public IActionResult CreateTable()
        {
            return View();
        }

        [HttpPost]
        public async Task<JsonResult> CreateTable(string tableName)
        {
            TableCreateHelper helper = new TableCreateHelper(this.amazonDynamoDBClient);

            if(tableName == TableNameConstants.Actor)
            {
                await helper.CreateActorTableAsync();
            }

            return new JsonResult(true) { };
        }

        public IActionResult InsertItem()
        {
            return View();
        }

        [HttpPost]
        public async Task<JsonResult> InsertItem(string tableName)
        {
            InsertItemHelper helper = new InsertItemHelper(this.amazonDynamoDBClient);

            if (tableName == TableNameConstants.Actor)
            {
                await helper.InsertItem(tableName);
            }

            return new JsonResult(true) { };
        }
    }
}
