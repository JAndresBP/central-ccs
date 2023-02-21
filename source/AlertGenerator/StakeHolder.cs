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
        try{
        if(_client != null){
            var request = new PublishRequest
                {
                    TopicArn = _topic,
                    Message = JsonConvert.SerializeObject(new {state, anomalies})
                };

            var response = await _client.PublishAsync(request);
        }else{
           await Task.Delay(1);
        }}catch(Exception e){
            Console.WriteLine(e);
        }
    }
}