namespace backend.Models;

public class Transaction
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string User { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Merchant { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty; // SUCCESS or FAILED
    public string Region { get; set; } = string.Empty;
    public DateTime Time { get; set; } = DateTime.UtcNow;
}
