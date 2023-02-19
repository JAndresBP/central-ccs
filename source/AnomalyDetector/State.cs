public class Localization{
    public double lat {get;set;} 
    public double lon {get;set;}
}
public enum VehicleStatus {
    Normal,
    Chrashed,
    UnplannedStop,
    PlannedStop,
    Emergency
}
public class State{
    public Guid signalId {get;set;}
    public Guid vehicleId {get;set;}
    public Localization? localization {get;set;}
    public double speed {get;set;}
    public double payloadTemperature {get;set;}
    public VehicleStatus status {get;set;}

    public async Task<IReadOnlyList<Anomaly>> CheckAnomalies(){
        var result = new List<Anomaly>();
        switch(this.status){
            case VehicleStatus.Chrashed:
                result.Add(Anomaly.Crashed);
            break;
            case VehicleStatus.UnplannedStop:
                result.Add(Anomaly.UnplannedStop);
            break;
            case VehicleStatus.Emergency:
                result.Add(Anomaly.Emergency);
            break;
        }
        
        if(this.speed > 100.0){
            result.Add(Anomaly.HighSpeed);
        }

        if(this.payloadTemperature > 45){
            result.Add(Anomaly.HighTemperature);
        }
        await Task.Delay(5);
        return result;
    }
}