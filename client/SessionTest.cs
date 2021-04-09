using System;
using System.Collections.Generic;
using iotdb_client_csharp.client.utils;

namespace iotdb_client_csharp.client
{
    public class SessionTest
    {
        public void TestOpen(){
            var session = new Session("localhost", 6667);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            System.Console.WriteLine("TestOpen Passed!");
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
        public void TestGetTimeZone(){
           var session = new Session("localhost", 6667);
           session.open(false);
           System.Diagnostics.Debug.Assert(session.is_open());
           var time_zone = session.get_time_zone(); 
           Console.WriteLine("TestGetTimeZone Passed!");
        }
        public void TestInsertStrRecord(){
           var session = new Session("localhost", 6667);
           int status = 0;
           session.open(false);
           System.Diagnostics.Debug.Assert(session.is_open());
           session.delete_storage_group("root.test_group");
           status = session.set_storage_group("root.test_group");
           System.Diagnostics.Debug.Assert(status == 0);
           session.delete_time_series("root.test_group.test_series");
           status = session.create_time_series("root.test_group.test_series", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);
           var measures = new List<string>{"s_01", "s_02"};
           var values = new List<string>{"test_record", "test_record"};
           status = session.insert_record("root.test_group.test_device", measures, values, 1);
           System.Diagnostics.Debug.Assert(status == 0);
           Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public void TestInsertRecord(){
           var session = new Session("localhost", 6667);
           int status = 0;
           session.open(false);
           System.Diagnostics.Debug.Assert(session.is_open());
           session.delete_storage_group("root.test_group");
           status = session.set_storage_group("root.test_group");
           System.Diagnostics.Debug.Assert(status == 0);
           session.delete_time_series("root.test_group.test_series");
           session.delete_time_series("root.test_group.test_device.test_series");
           status = session.create_time_series("root.test_group.test_device.test_series", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);
           var measures = new List<string>{"s_01"};
           var values = new List<string>{12.ToString()};
           var types = new List<TSDataType>{TSDataType.INT32};
           status = session.insert_record("root.test_group.test_device", measures, values, types, 1);
           System.Diagnostics.Debug.Assert(status == 0);
           Console.WriteLine("TestInsertRecord Passed!");
        }
        public void TestNonSql(){
           var session = new Session("localhost", 6667);
           session.open(false);
           System.Diagnostics.Debug.Assert(session.is_open());
           session.execute_non_query_statement("delete timeseries root.test_group.test_series");
           session.execute_non_query_statement("delete timeseries root.test_group.test_device.test_series");
           session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN");
           session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE");
           session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.hardware with datatype=TEXT,encoding=RLE");
           session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
           session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
           session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");


        }
        public void TestSqlQuery(){
           var session = new Session("localhost", 6667);
           int status = 0;
           session.open(false);
           System.Diagnostics.Debug.Assert(session.is_open());
           session.delete_storage_group("root.test_group");
           status = session.set_storage_group("root.test_group");
           System.Diagnostics.Debug.Assert(status == 0);
           session.delete_time_series("root.test_group.test_series");
           status = session.create_time_series("root.test_group.test_series", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);
           var res = session.execute_query_statement("show timeseries root");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           Console.WriteLine("SHOW TIMESERIES ROOT sql passed!");
           res = session.execute_query_statement("show devices");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           Console.WriteLine("SHOW DEVICES sql passed!");
           res = session.execute_query_statement("COUNT TIMESERIES root");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           Console.WriteLine("COUNT TIMESERIES root sql Passed");
           res=session.execute_query_statement("select * from root.ln.wf01 where time<10");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           Console.WriteLine("SELECT sql Passed");
        }


        static void Main(){
            var session_test = new SessionTest();
            session_test.TestOpen();
            //session_test.TestGetTimeZone();
            //session_test.TestInsertStrRecord();
            //session_test.TestInsertRecord();
            session_test.TestNonSql();
            session_test.TestSqlQuery();
        }
    }

}