using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using backend.Hubs;
using backend.Models;

namespace backend.Services;

public class AnomalyConsumerService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly IHubContext<DashboardHub> _hub;
    private readonly ILogger<AnomalyConsumerService> _logger;

    private readonly Dictionary<string, List<DateTime>> _userFailures = new();

    public AnomalyConsumerService(IConfiguration config, IHubContext<DashboardHub> hub, ILogger<AnomalyConsumerService> logger)
    {
        _config = config;
        _hub    = hub;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(
                KafkaClientHelper.GetConsumerConfig(_config, "anomaly-group")).Build();

            consumer.Subscribe(_config["Kafka:Topic"]);
            _logger.LogInformation("Anomaly consumer started...");

            while (!stoppingToken.IsCancellationRequested)
            {
                var result      = consumer.Consume(stoppingToken);
                var transaction = JsonSerializer.Deserialize<Transaction>(result.Message.Value);
                if (transaction != null) CheckForAnomaly(transaction, stoppingToken);
            }
        }, stoppingToken);
    }

    private void CheckForAnomaly(Transaction transaction, CancellationToken stoppingToken)
    {
        if (transaction.Status != "FAILED") return;

        if (!_userFailures.ContainsKey(transaction.User))
            _userFailures[transaction.User] = new List<DateTime>();

        _userFailures[transaction.User].Add(DateTime.UtcNow);

        var cutoff = DateTime.UtcNow.AddSeconds(-60);
        _userFailures[transaction.User] = _userFailures[transaction.User]
            .Where(t => t > cutoff).ToList();

        if (_userFailures[transaction.User].Count >= 3)
        {
            _hub.Clients.All.SendAsync("AnomalyDetected", new
            {
                User    = transaction.User,
                Message = $"⚠️ {transaction.User} failed {_userFailures[transaction.User].Count} times in the last 60 seconds",
                Time    = DateTime.UtcNow
            }, stoppingToken);

            _userFailures[transaction.User].Clear();
        }
    }
}
