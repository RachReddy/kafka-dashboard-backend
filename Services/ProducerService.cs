using System.Text.Json;
using Confluent.Kafka;
using backend.Models;

namespace backend.Services;

public class ProducerService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ILogger<ProducerService> _logger;
    private readonly ProducerState _state;

    private static readonly string[] Users     = ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "henry"];
    private static readonly string[] Merchants = ["Amazon", "Netflix", "Spotify", "Apple", "Nike", "Uber", "Airbnb", "Steam"];
    private static readonly string[] Regions   = ["US", "EU", "ASIA", "UK", "AU"];
    private static readonly decimal[] Amounts  = [9.99m, 14.99m, 29.99m, 49.99m, 99.99m, 149.99m, 249.99m, 499.99m];

    public ProducerService(IConfiguration config, ILogger<ProducerService> logger, ProducerState state)
    {
        _config = config;
        _logger = logger;
        _state  = state;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic  = _config["Kafka:Topic"];
        var random = new Random();

        using var producer = new ProducerBuilder<Null, string>(KafkaClientHelper.GetProducerConfig(_config)).Build();

        _logger.LogInformation("Producer ready. Waiting for start signal...");

        while (!stoppingToken.IsCancellationRequested)
        {
            if (!_state.IsRunning)
            {
                await Task.Delay(500, stoppingToken);
                continue;
            }

            var transaction = new Transaction
            {
                User     = Users[random.Next(Users.Length)],
                Amount   = Amounts[random.Next(Amounts.Length)],
                Merchant = Merchants[random.Next(Merchants.Length)],
                Status   = random.Next(10) < 8 ? "SUCCESS" : "FAILED",
                Region   = Regions[random.Next(Regions.Length)]
            };

            var message = JsonSerializer.Serialize(transaction);
            await producer.ProduceAsync(topic, new Message<Null, string> { Value = message }, stoppingToken);
            _logger.LogInformation("Published: {User} {Amount} {Status}", transaction.User, transaction.Amount, transaction.Status);

            await Task.Delay(2000, stoppingToken);
        }
    }
}
