// reading connection
var redisConnection = Environment.GetEnvironmentVariable("STATE_REDIS_CONNECTION") ?? "localhost:6379";

//get redis client
var redis = StackExchange.Redis.ConnectionMultiplexer.Connect(
    new StackExchange.Redis.ConfigurationOptions{
        EndPoints = { redisConnection },                
    });

var db = redis.GetDatabase();

const string streamName = "telemetry";

// setting up the server
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var app = builder.Build();

app.MapPost("/state",  async (State state) => {
    Console.WriteLine($"State signal received - signal Id: {state.signalId} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
    await db.StreamAddAsync(streamName,new StackExchange.Redis.NameValueEntry[]{
        new (nameof(State.signalId), state.signalId.ToString()),
        new (nameof(State.vehicleId), state.vehicleId.ToString()),
        // new (nameof(State.localization.lat), state.localization?.lat ?? 0),
        // new (nameof(State.localization.lon), state.localization?.lon ?? 0),
        new (nameof(State.speed),(double)state.speed),
        new (nameof(State.payloadTemperature),(double)state.payloadTemperature),
        new (nameof(State.status),(int)state.status) 
    });
    return Results.Accepted();
});

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.Run();

