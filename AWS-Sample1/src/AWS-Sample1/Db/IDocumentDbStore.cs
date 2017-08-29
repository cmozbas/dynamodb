using System;
using System.Threading.Tasks;

namespace AWS_Sample1.Db
{
    public interface IDocumentDbStore
    {
        Task<string> GetByIdAsync(string tableName, dynamic key, dynamic secondaryKey = null);

        Task<T> GetByIdAsync<T>(string tableName, dynamic key, dynamic secondaryKey = null);

        Task InsertOrUpdateAync(string tableName, string jsonModel);

        Task InsertOrUpdateAync<T>(string tableName, T model);
    }
}
