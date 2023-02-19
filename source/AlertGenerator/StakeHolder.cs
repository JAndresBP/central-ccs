using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Newtonsoft.Json;
public class StakeHolder{
    IAmazonSimpleNotificationService _client;
    string _topic;
    public StakeHolder(IAmazonSimpleNotificationService client, string topic)
    {
        _client = client;
        _topic = topic;
    }
    public async Task Notify(State state, IReadOnlyList<Anomaly> anomalies){
        var request = new PublishRequest
            {
                TopicArn = _topic,
                Message = JsonConvert.SerializeObject(new {state, anomalies})
            };

            var response = await _client.PublishAsync(request);
    }
}