using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using backend.Hubs;
using backend.Models;

namespace backend.Services;

public class FeedConsumerService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly IHubContext<DashboardHub> _hub;
    private readonly ILogger<FeedConsumerService> _logger;

    public FeedConsumerService(IConfiguration config, IHubContext<DashboardHub> hub, ILogger<FeedConsumerService> logger)
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
                KafkaClientHelper.GetConsumerConfig(_config, "feed-group")).Build();

            consumer.Subscribe(_config["Kafka:Topic"]);
            _logger.LogInformation("Feed consumer started...");

            while (!stoppingToken.IsCancellationRequested)
            {
                var result      = consumer.Consume(stoppingToken);
                var transaction = JsonSerializer.Deserialize<Transaction>(result.Message.Value);
                _hub.Clients.All.SendAsync("NewTransaction", transaction, stoppingToken);
            }
        }, stoppingToken);
    }
}
