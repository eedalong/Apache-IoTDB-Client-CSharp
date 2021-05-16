using Thrift.Transport;

namespace Apache.IoTDB
{
    public class Client
    {
        public TSIService.Client client;
        public long sessionId, statementId;
        public TFramedTransport transport;
    }
}