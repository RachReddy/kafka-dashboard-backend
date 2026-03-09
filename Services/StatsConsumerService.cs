using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using backend.Hubs;
using backend.Models;

namespace backend.Services;

// Consumer 2: reads every transaction, calculates running totals and pushes to the stat cards
public class StatsConsumerService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly IHubContext<DashboardHub> _hub;
    private readonly ILogger<StatsConsumerService> _logger;

    private decimal _totalAmount = 0;
    private int _totalCount = 0;
    private int _successCount = 0;
    private int _failureCount = 0;

    public StatsConsumerService(IConfiguration config, IHubContext<DashboardHub> hub, ILogger<StatsConsumerService> logger)
    {
        _config = config;
        _hub = hub;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"],
                GroupId          = "stats-group",
                AutoOffsetReset  = AutoOffsetReset.Latest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
            consumer.Subscribe(_config["Kafka:Topic"]);

            _logger.LogInformation("Stats consumer started...");

            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                var transaction = JsonSerializer.Deserialize<Transaction>(result.Message.Value);

                if (transaction == null) continue;

                // Update running totals
                _totalAmount += transaction.Amount;
                _totalCount++;

                if (transaction.Status == "SUCCESS") _successCount++;
                else _failureCount++;

                var successRate = _totalCount > 0
                    ? Math.Round((double)_successCount / _totalCount * 100, 1)
                    : 0;

                var stats = new
                {
                    TotalAmount  = _totalAmount,
                    TotalCount   = _totalCount,
                    SuccessRate  = successRate,
                    FailureCount = _failureCount
                };

                // Push updated stats to all connected browser clients
                _hub.Clients.All.SendAsync("StatsUpdated", stats, stoppingToken);
            }
        }, stoppingToken);
    }
}
