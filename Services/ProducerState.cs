namespace backend.Services;

// Shared singleton that controls whether the producer is running
public class ProducerState
{
    public bool IsRunning { get; set; } = false;
}
