using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class Client
    {
        public TSIService.Client client;
        public long sessionId, statementId;
    }
}