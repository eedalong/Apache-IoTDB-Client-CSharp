namespace iotdb_client_csharp.client
{
    public class Session
    {
       private string username="root", password="root", zoneId, host;
       private int port, fetch_size=10000;
       private long sessionId, statementId;

       public Session(string host, int port){
           // init success code 
           this.host = host;
           this.port = port;
       } 
        public Session(string host, int port, string username="root", string password="root", int fetch_size=10000, string zoneId = "UTC+08:00"){
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.zoneId = zoneId;
            this.fetch_size = fetch_size;
        }
        

       
    }
}