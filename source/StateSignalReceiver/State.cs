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
}