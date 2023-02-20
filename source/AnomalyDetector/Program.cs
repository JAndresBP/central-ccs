using StackExchange.Redis;
using Newtonsoft.Json;
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
            //var batchSize = int.Parse(Environment.GetEnvironmentVariable("BATCH_SIZE") ?? "1");
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
            Console.WriteLine($"Conectado a state cache - {stateRedisConnection} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");

            var alertDB = alertRedis.GetDatabase();
            Console.WriteLine($"Conectado a alert cache - {alertRedisConnection} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");

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
                    // string id = string.Empty;
                    while (!token.IsCancellationRequested)
                    {
                        try
                        {
                            // Console.WriteLine($"Reading - state cache - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
                            var result = await stateDb.StreamReadGroupAsync(streamName, groupName, $"avg-{i}", ">", 1);
                            if (result.Any())
                            {
                                var streamElement = result.First();

                                //counter = 0;
                                // var values = JsonConvert.SerializeObject(streamElement.Values);
                                // Console.WriteLine($"Detecting anomalies - {values} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");

                                var state = new State()
                                {
                                    signalId = Guid.Parse(Convert.ToString(streamElement[nameof(State.signalId)])),
                                    vehicleId = Guid.Parse(Convert.ToString(streamElement[nameof(State.vehicleId)])),
                                    // localization = new Localization()
                                    // {
                                    //     lat = Convert.ToDouble(streamElement[nameof(Localization.lat)]),
                                    //     lon = Convert.ToDouble(streamElement[nameof(Localization.lon)])
                                    // },
                                    speed = Convert.ToDouble(streamElement[nameof(State.speed)]),
                                    payloadTemperature = Convert.ToDouble(streamElement[nameof(State.payloadTemperature)]),
                                    status = (VehicleStatus)(Convert.ToInt32(streamElement[nameof(State.status)])),
                                    startTime = Convert.ToInt64(streamElement[nameof(State.startTime)])
                                };
                                var anomalies = await state.CheckAnomalies();
                                if (anomalies.Any())
                                {
                                    // Console.WriteLine($"Writting - alert cache - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
                                    var anomaliesStr = string.Join(",", anomalies.Select(item => item.ToString()));
                                    await alertDB.StreamAddAsync(alertStreamName, new StackExchange.Redis.NameValueEntry[]{
                                    new (nameof(State.signalId), state.signalId.ToString()),
                                    new (nameof(State.vehicleId), state.vehicleId.ToString()),
                                    // new (nameof(State.localization.lat), state.localization?.lat),
                                    // new (nameof(State.localization.lon), state.localization?.lon),
                                    new (nameof(State.speed),(double)state.speed),
                                    new (nameof(State.payloadTemperature),(double)state.payloadTemperature),
                                    new (nameof(State.status),(int)state.status),
                                    new (nameof(anomalies), anomaliesStr),
                                    new (nameof(State.startTime),state.startTime)
                                });
                                }
                                else
                                {
                                    var endtime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                    Console.WriteLine($"AnomalyDetector - total time ms - signal Id: {state.signalId} - start time: {state.startTime} - end time {endtime} delta time: {(endtime - state.startTime)}");
                                }

                                if (streamElement.Id.HasValue)
                                {
                                    await stateDb.StreamAcknowledgeAsync(streamName, groupName, streamElement.Id);
                                }

                            }
                            // counter++;
                            // if (counter > maxCount)
                            // {
                            //     tokensource.Cancel();
                            //     Console.WriteLine("!!!Closing!!!");
                            // }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }
                });
                consumerGroupTasks.Add(consumerGroupReadTask);
            }

            await Task.WhenAll(consumerGroupTasks);
        }
    }
}