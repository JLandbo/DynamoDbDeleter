using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows.Input;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace DynamoDbDeleter;

public class MainViewModel : INotifyPropertyChanged
{
    private string _accessKeyId = string.Empty;
    private string _secretAccessKey = string.Empty;
    private string _region = string.Empty;
    private string _tableName = string.Empty;
    private string _pkAttributeName = "PK";
    private string _pkValue = string.Empty;
    private string _skAttributeName = string.Empty;
    private string _skValue = string.Empty;
    private string _logText = string.Empty;
    private bool _isDeleting;
    private bool _isConnected;
    private bool _isConnecting;
    private string _connectionStatus = "Not connected";
    private bool _useScanMode;

    public string AccessKeyId
    {
        get => _accessKeyId;
        set { _accessKeyId = value; OnPropertyChanged(); }
    }

    public string SecretAccessKey
    {
        get => _secretAccessKey;
        set { _secretAccessKey = value; OnPropertyChanged(); }
    }

    public string Region
    {
        get => _region;
        set { _region = value; OnPropertyChanged(); }
    }

    public string TableName
    {
        get => _tableName;
        set { _tableName = value; OnPropertyChanged(); }
    }

    public string PkAttributeName
    {
        get => _pkAttributeName;
        set { _pkAttributeName = value; OnPropertyChanged(); }
    }

    public string PkValue
    {
        get => _pkValue;
        set { _pkValue = value; OnPropertyChanged(); }
    }

    public string SkAttributeName
    {
        get => _skAttributeName;
        set { _skAttributeName = value; OnPropertyChanged(); }
    }

    public string SkValue
    {
        get => _skValue;
        set { _skValue = value; OnPropertyChanged(); }
    }

    public string LogText
    {
        get => _logText;
        set { _logText = value; OnPropertyChanged(); }
    }

    private void Log(string message)
    {
        LogText += $"[{DateTime.Now:HH:mm:ss}] {message}\n";
    }

    public bool IsDeleting
    {
        get => _isDeleting;
        set { _isDeleting = value; OnPropertyChanged(); }
    }

    public bool IsConnected
    {
        get => _isConnected;
        set { _isConnected = value; OnPropertyChanged(); }
    }

    public bool IsConnecting
    {
        get => _isConnecting;
        set { _isConnecting = value; OnPropertyChanged(); }
    }

    public string ConnectionStatus
    {
        get => _connectionStatus;
        set { _connectionStatus = value; OnPropertyChanged(); }
    }

    public bool UseScanMode
    {
        get => _useScanMode;
        set { _useScanMode = value; OnPropertyChanged(); }
    }

    public ICommand DeleteCommand { get; }
    public ICommand ConnectCommand { get; }

    public MainViewModel()
    {
        DeleteCommand = new RelayCommand(DeleteItemAsync, () => !IsDeleting && IsConnected);
        ConnectCommand = new RelayCommand(ConnectAsync, () => !IsConnecting);
    }

    private async Task ConnectAsync()
    {
        if (string.IsNullOrWhiteSpace(AccessKeyId) || string.IsNullOrWhiteSpace(SecretAccessKey) || string.IsNullOrWhiteSpace(Region))
        {
            IsConnected = false;
            ConnectionStatus = "Missing credentials or region";
            return;
        }

        IsConnecting = true;
        ConnectionStatus = "Connecting...";

        try
        {
            var credentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey);
            var regionEndpoint = RegionEndpoint.GetBySystemName(Region);
            using var client = new AmazonDynamoDBClient(credentials, regionEndpoint);

            await client.ListTablesAsync(new ListTablesRequest { Limit = 1 });

            IsConnected = true;
            ConnectionStatus = "Connected";
        }
        catch (Exception ex)
        {
            IsConnected = false;
            ConnectionStatus = $"Failed: {ex.Message}";
        }
        finally
        {
            IsConnecting = false;
        }
    }

    private string? Validate()
    {
        if (string.IsNullOrWhiteSpace(AccessKeyId))
            return "Access Key ID is required.";
        if (string.IsNullOrWhiteSpace(SecretAccessKey))
            return "Secret Access Key is required.";
        if (string.IsNullOrWhiteSpace(Region))
            return "Region is required.";
        if (string.IsNullOrWhiteSpace(TableName))
            return "Table Name is required.";
        if (string.IsNullOrWhiteSpace(PkAttributeName))
            return "PK Attribute Name is required.";
        if (string.IsNullOrWhiteSpace(PkValue))
            return "PK Value is required.";
        if (string.IsNullOrWhiteSpace(SkAttributeName))
            return "SK Attribute Name is required for batch delete.";

        return null;
    }

    private async Task DeleteItemAsync()
    {
        var error = Validate();
        if (error != null)
        {
            Log(error);
            return;
        }

        IsDeleting = true;
        LogText = ""; // Clear log
        Log(UseScanMode ? "Scanning table (begins_with)..." : "Querying items (exact match)...");

        try
        {
            var credentials = new BasicAWSCredentials(AccessKeyId, SecretAccessKey);
            var regionEndpoint = RegionEndpoint.GetBySystemName(Region);
            using var client = new AmazonDynamoDBClient(credentials, regionEndpoint);

            var allKeys = UseScanMode 
                ? await ScanItemsAsync(client) 
                : await QueryItemsAsync(client);

            if (allKeys.Count == 0)
            {
                Log("No items found.");
                return;
            }

            // Confirmation dialog
            Log($"Found {allKeys.Count} items to delete.");
            var confirmResult = System.Windows.MessageBox.Show(
                $"You are about to delete {allKeys.Count} items.\n\nTable: {TableName}\nPK: {PkValue}\nSK: {(string.IsNullOrWhiteSpace(SkValue) ? "(all)" : SkValue)}\nMode: {(UseScanMode ? "Scan (begins_with)" : "Query (exact)")}\n\nAre you sure?",
                "Confirm Delete",
                System.Windows.MessageBoxButton.YesNo,
                System.Windows.MessageBoxImage.Warning);

            if (confirmResult != System.Windows.MessageBoxResult.Yes)
            {
                Log("Delete cancelled by user.");
                IsDeleting = false;
                return;
            }

            // Build delete requests
            var writeRequests = allKeys.Select(key => new WriteRequest
            {
                DeleteRequest = new DeleteRequest { Key = key }
            }).ToList();

            // Use batch delete service with parallel processing and retry logic
            var batchService = new BatchDeleteService(client, TableName, msg => 
                System.Windows.Application.Current.Dispatcher.Invoke(() => Log(msg)));
            await batchService.ExecuteBatchDeleteAsync(writeRequests);

            Log($"Deleted {allKeys.Count} items successfully.");
        }
        catch (Exception ex)
        {
            Log($"Error: {ex.Message}");
        }
        finally
        {
            IsDeleting = false;
        }
    }

    private async Task<List<Dictionary<string, AttributeValue>>> QueryItemsAsync(AmazonDynamoDBClient client)
    {
        var allKeys = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        // Build key condition - PK exact match, optionally SK exact match too
        var keyCondition = "#pk = :pkval";
        var attrNames = new Dictionary<string, string> { { "#pk", PkAttributeName }, { "#sk", SkAttributeName } };
        var attrValues = new Dictionary<string, AttributeValue> { { ":pkval", new AttributeValue { S = PkValue } } };

        if (!string.IsNullOrWhiteSpace(SkValue))
        {
            keyCondition += " AND #sk = :skval";
            attrValues[":skval"] = new AttributeValue { S = SkValue };
        }

        do
        {
            var queryRequest = new QueryRequest
            {
                TableName = TableName,
                KeyConditionExpression = keyCondition,
                ExpressionAttributeNames = attrNames,
                ExpressionAttributeValues = attrValues,
                ProjectionExpression = "#pk, #sk",
                ExclusiveStartKey = lastKey
            };

            var response = await client.QueryAsync(queryRequest);

            foreach (var item in response.Items)
            {
                allKeys.Add(new Dictionary<string, AttributeValue>
                {
                    { PkAttributeName, item[PkAttributeName] },
                    { SkAttributeName, item[SkAttributeName] }
                });
            }

            lastKey = response.LastEvaluatedKey;
            Log($"Found {allKeys.Count} items...");

        } while (lastKey != null && lastKey.Count > 0);

        return allKeys;
    }

    private async Task<List<Dictionary<string, AttributeValue>>> ScanItemsAsync(AmazonDynamoDBClient client)
    {
        var allKeys = new List<Dictionary<string, AttributeValue>>();
        Dictionary<string, AttributeValue>? lastKey = null;

        // Build filter expression for begins_with on PK (and optionally SK)
        var filterParts = new List<string> { "begins_with(#pk, :pkval)" };
        var attrNames = new Dictionary<string, string> { { "#pk", PkAttributeName }, { "#sk", SkAttributeName } };
        var attrValues = new Dictionary<string, AttributeValue> { { ":pkval", new AttributeValue { S = PkValue } } };

        if (!string.IsNullOrWhiteSpace(SkValue))
        {
            filterParts.Add("begins_with(#sk, :skval)");
            attrValues[":skval"] = new AttributeValue { S = SkValue };
        }

        do
        {
            var scanRequest = new ScanRequest
            {
                TableName = TableName,
                FilterExpression = string.Join(" AND ", filterParts),
                ExpressionAttributeNames = attrNames,
                ExpressionAttributeValues = attrValues,
                ProjectionExpression = "#pk, #sk",
                ExclusiveStartKey = lastKey
            };

            var response = await client.ScanAsync(scanRequest);

            foreach (var item in response.Items)
            {
                allKeys.Add(new Dictionary<string, AttributeValue>
                {
                    { PkAttributeName, item[PkAttributeName] },
                    { SkAttributeName, item[SkAttributeName] }
                });
            }

            lastKey = response.LastEvaluatedKey;
            Log($"Scanned... found {allKeys.Count} matching items so far...");

        } while (lastKey != null && lastKey.Count > 0);

        return allKeys;
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    protected void OnPropertyChanged([CallerMemberName] string? name = null)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}

public class RelayCommand : ICommand
{
    private readonly Func<Task> _execute;
    private readonly Func<bool>? _canExecute;

    public RelayCommand(Func<Task> execute, Func<bool>? canExecute = null)
    {
        _execute = execute;
        _canExecute = canExecute;
    }

    public bool CanExecute(object? parameter) => _canExecute?.Invoke() ?? true;

    public async void Execute(object? parameter) => await _execute();

    public event EventHandler? CanExecuteChanged
    {
        add => CommandManager.RequerySuggested += value;
        remove => CommandManager.RequerySuggested -= value;
    }
}
