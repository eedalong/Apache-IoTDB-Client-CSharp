using System.ComponentModel.DataAnnotations;
using System.Collections.Generic;
using System;
using iotdb_client_csharp.client.utils;

/*
group: root.97209_TEST_CSHARP_CLIENT_GROUP
timeseries: root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TSX;

*/
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
            System.Console.WriteLine("TestClose Passed!");

        }
        public void TestSetStorageGroup(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.set_storage_group("root.sg_test")==0);
            System.Console.WriteLine("TestSetStorageGroup Passed!");
            session.close();
        }
        public void TestDeleteStorageGroup(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            System.Diagnostics.Debug.Assert(session.delete_storage_group("root.sg_test")==0);
            System.Console.WriteLine("TestDeleteStorageGroup Passed!");
            session.close();
        }
        public void TestCreateTimeSeries(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.set_storage_group("root.sg_test");
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_01"), TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_02"), TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_03"), TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_04"), TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_05"), TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.sg_test.d_01.s_06"), TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Console.WriteLine("TestCreateTimeSeries Passed!");
            session.close();
        }
        public void TestCreateMultiTimeSeries(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.set_storage_group("root.sg_test");
            List<string> ts_path_lst = new List<string>(){"root.sg_test.d_01.s_01", "root.sg_test.d_01.s_02", "root.sg_test.d_01.s_03", "root.sg_test.d_01.s_04", "root.sg_test.d_01.s_05", "root.sg_test.d_01.s_06"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            System.Diagnostics.Debug.Assert(session.create_multi_time_series(ts_path_lst, data_type_lst, encoding_lst, compressor_lst)==0);
            System.Console.WriteLine("TestCreateMultiTimeSeries Passed!");
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
            System.Console.WriteLine("TestDeleteTimeSeries Passed!");
            session.delete_storage_group("root.sg_test");
            session.close();            
        }
        public void TestDeleteStorageGroups(){
            // by Luzhan
            var session = new Session("localhost", 6667, "root", "root");
            session.open(false);
            session.execute_query_statement("delete storage group root.TEST_CSHARP_CLIENT_GROUP");
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
            System.Console.WriteLine("TestDeleteStorageGroups Passed!");
            session.close();
        }
        public void TestGetTimeZone(){
           var session = new Session("localhost", 6667);
           session.open(false);
           session.execute_query_statement("delete storage group root.*");

           System.Diagnostics.Debug.Assert(session.is_open());
           var time_zone = session.get_time_zone(); 
           Console.WriteLine("TestGetTimeZone Passed!");
        }
        public void TestInsertStrRecord(){
           var session = new Session("localhost", 6667);
           int status = 0;
           session.open(false);
           session.execute_query_statement("delete storage group root.*");

           System.Diagnostics.Debug.Assert(session.is_open());
           session.delete_time_series("root.test_group.test_device.str1");
           session.delete_time_series("root.test_group.test_device.str2");
           status = session.create_time_series("root.test_group.test_device.str1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           status = session.create_time_series("root.test_group.test_device.str2", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);

           System.Diagnostics.Debug.Assert(status == 0);
           var measures = new List<string>{"str1", "str2"};
           var values = new List<string>{"test_record", "test_record"};
           status = session.insert_record("root.test_group.test_device", measures, values, 1);
           System.Diagnostics.Debug.Assert(status == 0);
           var res = session.execute_query_statement("select * from root.test_group.test_device.str* where time<2");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public void TestInsertRecord(){
            var session = new Session("localhost", 6667);
            int status = 0;
            session.open(false);
            session.execute_non_query_statement("delete storage group root.*");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.delete_time_series("root.test_group.test_device.ts1");
            session.delete_time_series("root.test_group.test_device.ts3");
            session.delete_time_series("root.test_group.test_device.ts2");
            
            status = session.create_time_series("root.test_group.test_device.ts1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.test_group.test_device.ts2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.test_group.test_device.ts3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"ts1", "ts2", "ts3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = session.insert_record("root.test_group.test_device", measures, values, types, 1);
            status = session.insert_record("root.test_group.test_device", measures, values, types, 2);
            status = session.insert_record("root.test_group.test_device", measures, values, types, 3);
            status = session.insert_record("root.test_group.test_device", measures, values, types, 4);
            var res = session.execute_query_statement("select * from root.test_group.test_device where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            System.Diagnostics.Debug.Assert(status == 0);
            Console.WriteLine("TestInsertRecord Passed!");
        }
        public void TestInsertRecords(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            List<string> device_id = new List<string>(){};
            for(int i = 0; i < 3; i++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            }
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<string>> values_lst = new List<List<string>>(){};
            values_lst.Add(new List<string>(){"true", 123.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString(), 10001.1.ToString(), "test_record"});
            List<List<TSDataType>> datatype_lst = new List<List<TSDataType>>(){};
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.TEXT});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};

            var status = session.insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine(status);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestInsertRecords Passed!");
        }
        public void TestTestInsertRecord(){
            var session = new Session("localhost", 6667);
            int status = 0;
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = session.test_insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 1);
            status = session.test_insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 2);
            status = session.test_insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 3);
            status = session.test_insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 4);
            var res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            System.Diagnostics.Debug.Assert(status == 0);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestTestInsertRecord Passed!");
        }

        public void TestTestInsertRecords(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            List<string> device_id = new List<string>(){};
            for(int i = 0; i < 3; i++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            }
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<string>> values_lst = new List<List<string>>(){};
            values_lst.Add(new List<string>(){"true", 123.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString(), 10001.1.ToString(), "test_record"});
            List<List<TSDataType>> datatype_lst = new List<List<TSDataType>>(){};
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.TEXT});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};

            var status = session.test_insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine(status);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestTestInsertRecords Passed!");
        }
        public void TestInsertRecordsOfOneDevice(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            var device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<string>> values_lst = new List<List<string>>(){};
            values_lst.Add(new List<string>(){"true", 123.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString()});
            values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString(), 10001.1.ToString(), "test_record"});
            List<List<TSDataType>> datatype_lst = new List<List<TSDataType>>(){};
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE});
            datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.TEXT});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};

            var status = session.insert_records_of_one_device(device_id, timestamp_lst, measurements_lst, datatype_lst, values_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine(status);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestInsertRecordsOfOneDevice Passed!");
        }
        public void TestCheckTimeSeriesExists(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            var ifExist_1 = session.check_time_series_exists("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1");
            var ifExist_2 = session.check_time_series_exists("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2");
            System.Diagnostics.Debug.Assert(ifExist_1 == true && ifExist_2 == false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestCheckTimeSeriesExists Passed!");
        }
        public void TestSetTimeZone(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.set_time_zone("GMT+8:00");
            System.Diagnostics.Debug.Assert(session.get_time_zone() == "GMT+8:00");
            session.close();
            Console.WriteLine("TestSetTimeZone Passed!");
        }
        public void TestDeleteData(){
            var session = new Session("localhost", 6667);
            int status = 0;
            session.open(false);
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.is_open());
            
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 1);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 2);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 3);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 4);
            var res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"};
            session.delete_data(ts_path_lst, 2 ,3);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            session.execute_non_query_statement("delete storage group root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();



            Console.WriteLine("TestDeleteData Passed!");
        }
        public void TestNonSql(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_query_statement("delete storage group root.*");
            System.Diagnostics.Debug.Assert(session.is_open());
            session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.ln.wf01.wt01.hardware with datatype=TEXT,encoding=PLAIN");
            session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, hardware) VALUES (7, true,'lz')");
            session.execute_non_query_statement("insert into root.ln.wf01.wt01(timestamp, status, hardware) VALUES (7, true,'lz')");


        }
        public void TestSqlQuery(){
            var session = new Session("localhost", 6667);
            session.open(false);
            session.execute_query_statement("delete storage group root.*");
            System.Diagnostics.Debug.Assert(session.is_open());
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
            res=session.execute_query_statement("select * from root.test_group.test_device where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine("SELECT sql Passed");

        }


        static void Main(){
            SessionTest session_test = new SessionTest();
            // session_test.TestOpen();
            // session_test.TestOpenFailure();
            // session_test.TestClose();
            // session_test.TestSetStorageGroup();
            // session_test.TestDeleteStorageGroup();
            // session_test.TestCreateTimeSeries();
            // session_test.TestDeleteTimeSeries();
            // session_test.TestCreateMultiTimeSeries();
            // session_test.TestDeleteStorageGroups();
            // session_test.TestGetTimeZone();
            // session_test.TestInsertStrRecord();
            // session_test.TestInsertRecord();
            // session_test.TestTestInsertRecord();
            // session_test.TestTestInsertRecords();
            // session_test.TestInsertRecordsOfOneDevice();
            // session_test.TestCheckTimeSeriesExists();
            // session_test.TestSetTimeZone();
            session_test.TestDeleteData();
            // session_test.TestNonSql();
            // session_test.TestSqlQuery();
            // session_test.TestInsertRecords();
            System.Console.WriteLine("TEST PASSED");

        }
    }

}