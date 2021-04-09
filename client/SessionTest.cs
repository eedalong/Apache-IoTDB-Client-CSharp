using System.Collections.Generic;
using System;
namespace iotdb_client_csharp.client
{
    public class SessionTest
    {
        public void TestOpen(){
            Session session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            System.Console.WriteLine("Test open Passed!");
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
            Console.WriteLine("All tests passed!");
        }
    }

}