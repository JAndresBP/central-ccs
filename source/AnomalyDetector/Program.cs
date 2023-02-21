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
            var batchSize = int.Parse(Environment.GetEnvironmentVariable("BATCH_SIZE") ?? "1");
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
                    string consumer = $"{Guid.NewGuid().ToString()}{i}";
                    var sw = new System.Diagnostics.Stopwatch();
                    sw.Start();
                    while (!token.IsCancellationRequested)
                    {
                        var sw1 = sw.ElapsedMilliseconds;
                        try
                        {
                            // Console.WriteLine($"Reading - state cache - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
                            var result = await stateDb.StreamReadGroupAsync(streamName, groupName, consumer, ">", batchSize);
                            var sw2 = sw.ElapsedMilliseconds;
                            if (result.Any())
                            {
                                foreach (var streamElement in result)
                                {

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
                                    var sw3 = sw.ElapsedMilliseconds;
                                    if (anomalies.Any())
                                    {
                                        // Console.WriteLine($"Writting - alert cache - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
                                        var anomaliesStr = string.Join(",", anomalies.Select(item => item.ToString()));
                                        await alertDB.StreamAddAsync(alertStreamName, streamElement.Values.Append(new(nameof(anomalies), anomaliesStr)).ToArray());
                                        var sw4 = sw.ElapsedMilliseconds;
                                        Console.WriteLine($"Anomaly detector add alert elapsed(ms): {sw4 - sw3}");
                                    }
                                    else
                                    {
                                        var endtime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                                        Console.WriteLine($"AnomalyDetector - total time ms - signal Id: {state.signalId} - start time: {state.startTime} - end time {endtime} delta time: {(endtime - state.startTime)}");
                                    }
                                    var sw5 = sw.ElapsedMilliseconds;
                                    if (streamElement.Id.HasValue)
                                    {
                                        await stateDb.StreamAcknowledgeAsync(streamName, groupName, streamElement.Id);
                                    }
                                    var sw6 = sw.ElapsedMilliseconds;

                                    Console.WriteLine($"Anomaly detector reading state elapsed(ms): {sw2 - sw1}");
                                    Console.WriteLine($"Anomaly detector elapsed(ms): {sw3 - sw2}");
                                    Console.WriteLine($"Anomaly detector ack elapsed(ms): {sw6 - sw5}");
                                }
                                // counter++;
                                // if (counter > maxCount)
                                // {
                                //     tokensource.Cancel();
                                //     Console.WriteLine("!!!Closing!!!");
                                // }
                            }
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