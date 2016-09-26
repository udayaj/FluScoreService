package fluscoreservice;

import java.util.Date;

/**
 *
 * @author udaya
 */
public class MsgData {     
    public int MsgId;
    public String Msg;   
    public String KeyWords;
    public float Longitude;
    public float Latitude;
    public java.util.Date CreatedTime;
    public String PlaceName;    
    public String PlacePolygon;
    public int RetweetCount;
    public String UserLocation;
    public float TagScore;
    public float NormalizedScore;
    public float CombinedScore;
    public boolean IsQualified = false;
    public boolean IsBlackListed = false;    
    public float WUP = 0;
    public float RES = 0;
    public float WUP_Flu = 0;
    public float RES_Flu = 0;
    
    
    public MsgData(int MsgId, String Msg, String KeyWords, float Longitude, float Latitude, Date CreatedTime, String PlaceName, String PlacePolygon, int RetweetCount, String UserLocation, float TagScore, float NormalizedScore, float CombinedScore, boolean IsQualified, boolean IsBlackListed) {
        this.MsgId = MsgId;
        this.Msg = Msg;     
        this.KeyWords = KeyWords;  
        this.Longitude = Longitude;
        this.Latitude = Latitude;
        this.CreatedTime = CreatedTime;        
        this.PlaceName = PlaceName;        
        this.PlacePolygon = PlacePolygon;
        this.RetweetCount = RetweetCount;
        this.UserLocation = UserLocation;        
        this.TagScore = TagScore;
        this.NormalizedScore = NormalizedScore;
        this.CombinedScore = CombinedScore;
        this.IsQualified = IsQualified;
        this.IsBlackListed = IsBlackListed;
    }
    
}
