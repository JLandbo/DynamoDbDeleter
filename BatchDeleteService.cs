using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Collections.Concurrent;

namespace DynamoDbDeleter;

public class BatchDeleteService(IAmazonDynamoDB client, string tableName, Action<string> onProgress)
{
    public async Task ExecuteBatchDeleteAsync(List<WriteRequest> writeRequests, int retryCount = 0, int maxRetries = 10)
    {
        if (writeRequests.Count == 0) return;

        var chunks = writeRequests.Chunk(25).ToList();
        onProgress($"Processing {chunks.Count} chunks ({writeRequests.Count} items)...");

        var unprocessedItems = new ConcurrentQueue<WriteRequest>();
        var processedCount = 0;

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4
        };

        await Parallel.ForEachAsync(chunks, parallelOptions, async (chunk, _) =>
        {
            var response = await ProcessChunkAsync([.. chunk]);
            if (response.Count != 0)
            {
                foreach (var item in response)
                {
                    unprocessedItems.Enqueue(item);
                }
            }
            Interlocked.Add(ref processedCount, chunk.Length - response.Count);
            onProgress($"Deleted {processedCount} of {writeRequests.Count} items...");
        });

        if (!unprocessedItems.IsEmpty)
        {
            retryCount++;
            if (retryCount >= maxRetries)
            {
                throw new Exception($"Failed to process {unprocessedItems.Count} items after {maxRetries} retries.");
            }

            int delay = Math.Min(1000 * (1 << retryCount), 10_000);
            int jitter = Random.Shared.Next(100, 500);
            onProgress($"Retrying {unprocessedItems.Count} unprocessed items (retry {retryCount}/{maxRetries})...");
            await Task.Delay(delay + jitter);

            await ExecuteBatchDeleteAsync([.. unprocessedItems], retryCount, maxRetries);
        }
    }

    private async Task<List<WriteRequest>> ProcessChunkAsync(List<WriteRequest> chunk)
    {
        var batchRequest = new BatchWriteItemRequest
        {
            RequestItems = new Dictionary<string, List<WriteRequest>>
            {
                { tableName, chunk }
            }
        };

        try
        {
            var response = await client.BatchWriteItemAsync(batchRequest);
            return response.UnprocessedItems.GetValueOrDefault(tableName) ?? [];
        }
        catch (ProvisionedThroughputExceededException)
        {
            return chunk; // Retry the whole chunk
        }
    }
}
