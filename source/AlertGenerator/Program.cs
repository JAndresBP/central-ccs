using StackExchange.Redis;
using Amazon.SimpleNotificationService;
using Newtonsoft.Json;

namespace AlertGenerator
{

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var tokensource = new CancellationTokenSource();
            var token = tokensource.Token;

            var alertRedisConnection = Environment.GetEnvironmentVariable("ALERT_REDIS_CONNECTION") ?? "localhost:6380";
            var threads = int.Parse(Environment.GetEnvironmentVariable("THREADS") ?? "1");

            var alertRedis = ConnectionMultiplexer.Connect(
                new ConfigurationOptions
                {
                    EndPoints = { alertRedisConnection },
                });

            var alertDB = alertRedis.GetDatabase();
            Console.WriteLine($"Conectado a alert cache - {alertRedisConnection} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
            const string alertStreamName = "alerts";
            const string groupName = "avg";

            if (!(await alertDB.KeyExistsAsync(alertStreamName)) ||
                (await alertDB.StreamGroupInfoAsync(alertStreamName)).All(x => x.Name != groupName))
            {
                await alertDB.StreamCreateConsumerGroupAsync(alertStreamName, groupName, "0-0", true);
            }
            IAmazonSimpleNotificationService client = null;

            try
            {
                client = new AmazonSimpleNotificationServiceClient();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            string topicArn = "arn:aws:sns:us-east-1:439979626637:interesados";

            var stakeholders = new List<StakeHolder>();
            for (int i = 0; i < 1; i++)
            {
                stakeholders.Add(new StakeHolder(client, topicArn));
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
                        // Console.WriteLine($"Reading - alert cache - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
                        if (!string.IsNullOrEmpty(id))
                        {
                            await alertDB.StreamAcknowledgeAsync(alertStreamName, groupName, id);
                            id = string.Empty;
                        }
                        var result = await alertDB.StreamReadGroupAsync(alertStreamName, groupName, $"avg-{i}", ">", 1);
                        if (result.Any())
                        {
                            try
                            {
                                var streamElement = result.First();
                                var values = JsonConvert.SerializeObject(streamElement.Values);
                                Console.WriteLine($"Sending alerts - {values} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");

                                id = Convert.ToString(streamElement.Id);
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
                                    status = (VehicleStatus)Convert.ToInt32(streamElement[nameof(State.status)]),
                                };
                                IReadOnlyList<Anomaly> anomalies;
                                anomalies = (Convert.ToString(streamElement[nameof(anomalies)])).Split(',').Select(item => Enum.Parse<Anomaly>(item)).ToList();
                                await NotifyAllStakeHolders(stakeholders, state, anomalies);
                            }
                            catch (Exception e) {
                                Console.WriteLine(e);
                            }
                        }
                        // counter++;
                        // if (counter > maxCount)
                        // {
                        //     tokensource.Cancel();
                        // }
                    }
                });
                consumerGroupTasks.Add(consumerGroupReadTask);
            }
            await Task.WhenAll(consumerGroupTasks);
        }

        public static async Task NotifyAllStakeHolders(IReadOnlyList<StakeHolder> stakeHolders, State state, IReadOnlyList<Anomaly> anomalies)
        {
            Console.WriteLine($"Sending Alerts - signal Id: {state.signalId} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
            var tasks = stakeHolders.Select(item => item.Notify(state, anomalies));
            await Task.WhenAll(tasks.ToArray());
            //Console.WriteLine($"Sending Alerts complete - signal Id: {state.signalId} - time: {System.Diagnostics.Stopwatch.GetTimestamp()}");
        }

    }
}