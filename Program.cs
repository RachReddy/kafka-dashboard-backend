using backend.Hubs;
using backend.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ProducerState>();
builder.Services.AddSignalR();
builder.Services.AddHostedService<ProducerService>();
builder.Services.AddHostedService<FeedConsumerService>();
builder.Services.AddHostedService<StatsConsumerService>();
builder.Services.AddHostedService<AnomalyConsumerService>();

// FRONTEND_URL env var is set on Render. Falls back to localhost for local dev.
var frontendUrl = Environment.GetEnvironmentVariable("FRONTEND_URL") ?? "http://localhost:5173";

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(frontendUrl, "http://localhost:5173", "http://localhost:5174")
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials();
    });
});

var app = builder.Build();

app.UseCors();
app.MapHub<DashboardHub>("/hub");

app.MapPost("/producer/start", (ProducerState state) =>
{
    state.IsRunning = true;
    return Results.Ok(new { status = "started" });
});

app.MapPost("/producer/stop", (ProducerState state) =>
{
    state.IsRunning = false;
    return Results.Ok(new { status = "stopped" });
});

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
