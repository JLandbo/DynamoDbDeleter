using System.Windows;

namespace DynamoDbDeleter;

public partial class MainWindow : Window
{
    private readonly MainViewModel _viewModel;

    public MainWindow()
    {
        InitializeComponent();
        _viewModel = new MainViewModel();
        DataContext = _viewModel;
    }

    private void SecretAccessKeyBox_PasswordChanged(object sender, RoutedEventArgs e)
    {
        _viewModel.SecretAccessKey = SecretAccessKeyBox.Password;
    }
}
