using StackExchange.Redis;

namespace AnomalyDetector
{

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var tokensource = new CancellationTokenSource();
            var token = tokensource.Token;

            var stateRedisConnection = Environment.GetEnvironmentVariable("STATE_REDIS_CONNECTION") ?? "localhost:6379";
            var alertRedisConnection = Environment.GetEnvironmentVariable("ALERT_REDIS_CONNECTION") ?? "localhost:6380";
            var threads = int.Parse(Environment.GetEnvironmentVariable("THREADS") ?? "1");
            var stateRedis = ConnectionMultiplexer.Connect(
                new ConfigurationOptions
                {
                    EndPoints = { stateRedisConnection },
                });

            var alertRedis = ConnectionMultiplexer.Connect(
                new ConfigurationOptions
                {
                    EndPoints = { alertRedisConnection },
                });

            var stateDb = stateRedis.GetDatabase();
            var alertDB = alertRedis.GetDatabase();

            const string alertStreamName = "alerts";
            const string streamName = "telemetry";
            const string groupName = "avg";

            if (!(await stateDb.KeyExistsAsync(streamName)) ||
                (await stateDb.StreamGroupInfoAsync(streamName)).All(x => x.Name != groupName))
            {
                await stateDb.StreamCreateConsumerGroupAsync(streamName, groupName, "0-0", true);
            }
            List<Task> consumerGroupTasks = new List<Task>();
            for (int i = 1; i <= threads; i++)
            {
                var consumerGroupReadTask = Task.Run(async () =>
                {
                    var counter = 0;
                    const int maxCount = 6000000;
                    string id = string.Empty;
                    while (!token.IsCancellationRequested)
                    {
                        if (!string.IsNullOrEmpty(id))
                        {
                            await stateDb.StreamAcknowledgeAsync(streamName, groupName, id);
                            id = string.Empty;
                        }
                        var result = await stateDb.StreamReadGroupAsync(streamName, groupName, $"avg-{i}", ">", 1);
                        if (result.Any())
                        {
                            try
                            {
                                counter = 0;
                                var streamElement = result.First();
                                id = streamElement.Id;
                                var state = new State()
                                {
                                    signalId = Guid.Parse(streamElement[nameof(State.signalId)]),
                                    vehicleId = Guid.Parse(streamElement[nameof(State.vehicleId)]),
                                    localization = new Localization()
                                    {
                                        lat = (double)streamElement[nameof(Localization.lat)],
                                        lon = (double)streamElement[nameof(Localization.lon)]
                                    },
                                    speed = (double)streamElement[nameof(State.speed)],
                                    payloadTemperature = (double)streamElement[nameof(State.payloadTemperature)],
                                    status = (VehicleStatus)(int)streamElement[nameof(State.status)],
                                };
                                var anomalies = await state.CheckAnomalies();
                                if (anomalies.Any())
                                {
                                    var anomaliesStr = string.Join(",", anomalies.Select(item => item.ToString()));
                                    await alertDB.StreamAddAsync(alertStreamName, new StackExchange.Redis.NameValueEntry[]{
                                new (nameof(State.signalId), state.signalId.ToString()),
                                new (nameof(State.vehicleId), state.vehicleId.ToString()),
                                new (nameof(State.localization.lat), state.localization?.lat),
                                new (nameof(State.localization.lon), state.localization?.lon),
                                new (nameof(State.speed),(double)state.speed),
                                new (nameof(State.payloadTemperature),(double)state.payloadTemperature),
                                new (nameof(State.status),(int)state.status),
                                new (nameof(anomalies), anomaliesStr)
                                });
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }

                        }
                        counter++;
                        if (counter > maxCount)
                        {
                            tokensource.Cancel();
                        }
                    }
                });
                consumerGroupTasks.Add(consumerGroupReadTask);
            }

            await Task.WhenAll(consumerGroupTasks);
        }
    }
}