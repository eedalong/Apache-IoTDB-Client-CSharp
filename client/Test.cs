using iotdb_client_csharp.client.test;
using System;
namespace iotdb_client_csharp.client
{
    public class Test
    {
        static void Main(){
            // Unit Test
            UnitTest unit_test = new UnitTest();
            unit_test.Test();
            // Session Test
            SessionTest session_test = new SessionTest();
            session_test.Test();

            Console.WriteLine("TEST PASSED");

        }
        
    }
}