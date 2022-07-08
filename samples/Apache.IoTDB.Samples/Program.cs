using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Apache.IoTDB.Samples
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var sessionPoolTest = new SessionPoolTest("iotdb");
           await  sessionPoolTest.Test() ;
        }

        public static void OpenDebugMode(this SessionPool session)
        {
            session.OpenDebugMode(builder =>
            {
                builder.AddConsole();
                builder.AddNLog();
            });
        }
    }
}