using System.Collections.Generic;
using System;
using System.Collections.Generic;
using iotdb_client_csharp.client.utils;

namespace iotdb_client_csharp.client
{
    public class SessionTest
    {
        public void TestOpen(){
            Session session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            System.Console.WriteLine("TestOpen Passed!");
        }
        public void TestOpenFailure(){
            // by Luzhan
            // TODO 后续所有的失败测例一起处理吧
            // var session = new Session("localhost", 6667, "root", "root");
            // session.open(false);
            // System.Diagnostics.Debug.Assert(!session.is_open());
            // System.Console.WriteLine("Test open failure Passed!");
        }
        public void TestClose(){
            
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.close();
            System.Diagnostics.Debug.Assert(!session.is_open());
            System.Console.WriteLine("Test close Passed!");

        }
        public void TestSetStorageGroup(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.set_storage_group("root.sg_test")==0);
            System.Console.WriteLine("Test set storage group Passed!");
            session.close();
        }
        public void TestDeleteStorageGroup(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.delete_storage_group("root.sg_test")==0);
            System.Console.WriteLine("Test delete storage group Passed!");
            session.close();
        }
        public void TestCreateTimeSeries(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.set_storage_group("root.sg_test");
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_01"), utils.TSDataType.BOOLEAN, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_02"), utils.TSDataType.INT32, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_03"), utils.TSDataType.INT64, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_04"), utils.TSDataType.FLOAT, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_05"), utils.TSDataType.DOUBLE, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_06"), utils.TSDataType.TEXT, utils.TSEncoding.PLAIN, utils.Compressor.SNAPPY)==0);
            System.Console.WriteLine("Test create timeseries Passed!");
            session.close();
        }
        public void TestCreateMultiTimeSeries(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.set_storage_group("root.sg_test");
            List<string> ts_path_lst = new List<string>(){"root.sg_test.d_01.s_01", "root.sg_test.d_01.s_02", "root.sg_test.d_01.s_03", "root.sg_test.d_01.s_04", "root.sg_test.d_01.s_05", "root.sg_test.d_01.s_06"};
            List<utils.TSDataType> data_type_lst = new List<utils.TSDataType>(){utils.TSDataType.BOOLEAN, utils.TSDataType.INT32, utils.TSDataType.INT64, utils.TSDataType.FLOAT, utils.TSDataType.DOUBLE, utils.TSDataType.TEXT};
            List<utils.TSEncoding> encoding_lst = new List<utils.TSEncoding>(){utils.TSEncoding.PLAIN,utils.TSEncoding.PLAIN,utils.TSEncoding.PLAIN,utils.TSEncoding.PLAIN,utils.TSEncoding.PLAIN,utils.TSEncoding.PLAIN};
            List<utils.Compressor> compressor_lst = new List<utils.Compressor>(){utils.Compressor.SNAPPY,utils.Compressor.SNAPPY,utils.Compressor.SNAPPY,utils.Compressor.SNAPPY,utils.Compressor.SNAPPY,utils.Compressor.SNAPPY};
            System.Diagnostics.Debug.Assert(session.create_multi_time_series(ts_path_lst, data_type_lst, encoding_lst, compressor_lst)==0);
            System.Console.WriteLine("Test create multi timeseries Passed!");
            session.delete_time_series(ts_path_lst);
            session.delete_storage_group("root.sg_test");
            session.close();      
        }
        public void TestDeleteTimeSeries(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            List<string> ts_path_lst = new List<string>(){};
            ts_path_lst.Add("root.sg_test.d_01.s_01");
            ts_path_lst.Add("root.sg_test.d_01.s_02");
            ts_path_lst.Add("root.sg_test.d_01.s_03");
            ts_path_lst.Add("root.sg_test.d_01.s_04");
            ts_path_lst.Add("root.sg_test.d_01.s_05");
            ts_path_lst.Add("root.sg_test.d_01.s_06");
            System.Diagnostics.Debug.Assert(session.delete_time_series(ts_path_lst)==0);
            System.Console.WriteLine("Test delete timeseries Passed!");
            session.delete_storage_group("root.sg_test");
            session.close();            
        }
        public void TestDeleteStorageGroups(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.set_storage_group("root.sg_test_01");
            session.set_storage_group("root.sg_test_02");
            session.set_storage_group("root.sg_test_03");
            session.set_storage_group("root.sg_test_04");
            List<string> group_names = new List<string>(){};
            group_names.Add("root.sg_test_01");
            group_names.Add("root.sg_test_02");
            group_names.Add("root.sg_test_03");
            group_names.Add("root.sg_test_04");
            System.Diagnostics.Debug.Assert(session.delete_storage_groups(group_names)==0);
            System.Console.WriteLine("Test delete storage groups Passed!");
            session.close();
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
            SessionTest session_test = new SessionTest();
            session_test.TestOpen();
            session_test.TestOpenFailure();
            session_test.TestClose();
            session_test.TestSetStorageGroup();
            session_test.TestDeleteStorageGroup();
            session_test.TestCreateTimeSeries();
            session_test.TestDeleteTimeSeries();
            session_test.TestCreateMultiTimeSeries();
            session_test.TestDeleteStorageGroups();
            session_test.TestGetTimeZone();
            session_test.TestInsertStrRecord();
            session_test.TestInsertRecord();
            session_test.TestNonSql();
            session_test.TestSqlQuery();
        }
    }

}