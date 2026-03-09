using Microsoft.AspNetCore.SignalR;

namespace backend.Hubs;

// Clients connect to this hub from the browser.
// Consumers call this hub to broadcast data to all connected clients.
public class DashboardHub : Hub
{
    // Called when a browser client connects
    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Connected", "Dashboard connected successfully");
        await base.OnConnectedAsync();
    }
}
