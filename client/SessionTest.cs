using System;
namespace iotdb_client_csharp.client
{
    public class SessionTest
    {
        public void TestOpen(){
            var session = new Session("localhost", 8001);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            System.Console.WriteLine("Test Passed!");
        }
        
    
        public void main(){
            TestOpen();
        }
    }

}