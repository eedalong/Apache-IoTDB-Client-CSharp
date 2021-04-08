using System;
namespace iotdb_client_csharp.client
{
    public class SessionTest
    {
        public void TestOpen(){
            var session = new Session("localhost", 6667);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            System.Console.WriteLine("Test Passed!");
        }
        public void TestOpenFailuer(){
            // by Luzhan
        }
        public void TestClose(){
            // by Luzhan
        }
        public void TestCreateStorageGroup(){
            // by Luzhan
        }
        public void TestDeleteStorageGroup(){
            // by Luzhan
        }
        public void TestCreateTimeSeries(){
            // by Luzhan
        }
        public void TestCreateMultiTimeSeries(){
            // by Luzhan
        }
        public void TestDeleteTimeSeries(){
            // by Luzhan
        }
        public void TestDeleteStorageGroups(){
            // by Luzhan
        }
        
        static void Main(){
            var session_test = new SessionTest();
            session_test.TestOpen();
            session_test.TestOpenFailuer();
        }
    }

}