using Confluent.Kafka;

namespace backend.Services;

public static class KafkaClientHelper
{
    public static ProducerConfig GetProducerConfig(IConfiguration config)
    {
        var cfg = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"]
        };

        ApplySecurity(cfg, config);
        return cfg;
    }

    public static ConsumerConfig GetConsumerConfig(IConfiguration config, string groupId)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            GroupId          = groupId,
            AutoOffsetReset  = AutoOffsetReset.Latest
        };

        ApplySecurity(cfg, config);
        return cfg;
    }

    // Only applies SASL if credentials are present — plaintext otherwise
    private static void ApplySecurity(ClientConfig cfg, IConfiguration config)
    {
        var username = config["Kafka:Username"];
        var password = config["Kafka:Password"];

        if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
        {
            cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
            cfg.SaslMechanism    = SaslMechanism.ScramSha256;
            cfg.SaslUsername     = username;
            cfg.SaslPassword     = password;
        }
    }
}
