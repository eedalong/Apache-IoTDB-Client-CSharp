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
        public string host = "localhost";
        public int port = 6667;
        public string user = "root";
        public string passwd = "root";
        public int fetch_size = 5000;
        public bool debug = false;
        public void TestOpen(){
            Session session = new Session(host, port, user, passwd);
            if(debug){
                session.open_debug_mode();
            }
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            session.close();
            Console.WriteLine("TestOpen Passed!");
        }

        public void TestClose(){
            var session = new Session(host, port, user, passwd);
            session.open(false);
            session.close();
            System.Diagnostics.Debug.Assert(!session.is_open());
            Console.WriteLine("TestClose Passed!");

        }
        public void TestSetAndDeleteStorageGroup(){
            // by Luzhan
            var session = new Session(host, port, user, passwd);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            System.Diagnostics.Debug.Assert(session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            session.close();
            Console.WriteLine("TestSetAndDeleteStorageGroup Passed!");
        }
        public void TestCreateTimeSeries(){
            // by Luzhan
            var session = new Session(host, port, user, passwd);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1"), TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"), TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3"), TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4"), TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5"), TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(session.create_time_series(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"), TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestCreateTimeSeries Passed!");
        }
        public void TestCreateMultiTimeSeries(){
            // by Luzhan
            var session = new Session(host, port, user, passwd);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }

            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            System.Diagnostics.Debug.Assert(session.create_multi_time_series(ts_path_lst, data_type_lst, encoding_lst, compressor_lst)==0);
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();      
            Console.WriteLine("TestCreateMultiTimeSeries Passed!");

        }
        public void TestDeleteTimeSeries(){
            // by Luzhan
            var session = new Session(host, port, user, passwd);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            System.Diagnostics.Debug.Assert(session.create_multi_time_series(ts_path_lst, data_type_lst, encoding_lst, compressor_lst)==0);
            System.Diagnostics.Debug.Assert(session.delete_time_series(ts_path_lst)==0);
            Console.WriteLine("TestDeleteTimeSeries Passed!");
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();            
        }
        public void TestDeleteStorageGroups(){
            // by Luzhan
            var session = new Session(host, port, user, passwd);
            session.open(false); 
            if(debug){
                session.open_debug_mode();
            }
           
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            session.set_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            List<string> group_names = new List<string>(){};
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            System.Diagnostics.Debug.Assert(session.delete_storage_groups(group_names)==0);
            session.close();
            Console.WriteLine("TestDeleteStorageGroups Passed!");
        }
        public void TestGetTimeZone(){
           var session = new Session(host, port);
           session.open(false);
           if(debug){
                session.open_debug_mode();
            }
           session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
           System.Diagnostics.Debug.Assert(session.is_open());
           var time_zone = session.get_time_zone();
           session.close();
           Console.WriteLine("TestGetTimeZone Passed!");
        }
        public void TestInsertStrRecord(){
           var session = new Session(host, port);
           int status = 0;
           session.open(false);
           if(debug){
                session.open_debug_mode();
            }
           System.Diagnostics.Debug.Assert(session.is_open());
           session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");

           status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);

           var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"};
           var values = new List<string>{"test_record", "test_record"};
           status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, 1);
           System.Diagnostics.Debug.Assert(status == 0);

           var res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<2");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }

           // large data test
           for(int timestamp = 2; timestamp <=fetch_size * 4; timestamp++){
               session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, timestamp);
           }
           res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
           int res_count = 0;
           while(res.has_next()){
               res.next();
               res_count += 1;
           }
           System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
           session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
           session.close();
           Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public void TestInsertRecord(){
            var session = new Session(host, port);
            int status = 0;
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");

            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 1);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 2);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 3);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 4);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            for(int timestamp = 5; timestamp <= fetch_size * 4; timestamp++){
                session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, timestamp);
            }
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestInsertRecord Passed!");
        }
        public void TestInsertRecords(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            
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
            status = session.insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine(status);

            // large data test
            device_id = new List<string>(){};
            values_lst = new List<List<string>>(){};
            measurements_lst = new List<List<string>>(){};
            datatype_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<long>(){};
            for(int timestamp = 4;timestamp <= fetch_size * 4;timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
                values_lst.Add(new List<string>(){"true", 123.ToString()});
                measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
                datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(timestamp);
            }
            status = session.insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int record_count = fetch_size * 4;
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == record_count);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestInsertRecords Passed!");
        }
        public void TestTestInsertRecord(){
            var session = new Session(host, port);
            int status = 0;
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
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
            // large data test
            for(int timestamp = 5; timestamp <= fetch_size * 4; timestamp++){
                session.test_insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, timestamp);
            }
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestTestInsertRecord Passed!");
        }

        public void TestTestInsertRecords(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");

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

            status = session.test_insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            device_id = new List<string>(){};
            values_lst = new List<List<string>>(){};
            measurements_lst = new List<List<string>>(){};
            datatype_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<long>(){};
            for(int timestamp = 4;timestamp <= fetch_size * 4;timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
                values_lst.Add(new List<string>(){"true", 123.ToString()});
                measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
                datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(timestamp);
            }
            status = session.test_insert_records(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestTestInsertRecords Passed!");
        }
        public void TestInsertRecordsOfOneDevice(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
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

            status = session.insert_records_of_one_device(device_id, timestamp_lst, measurements_lst, datatype_lst, values_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            measurements_lst = new List<List<string>>(){};
            values_lst = new List<List<string>>(){};
            datatype_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<long>(){};
            for(int timestamp = 4; timestamp <= fetch_size * 4;timestamp++){
                measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
                values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString(), 10001.1.ToString(), "test_record"});
                datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.TEXT});
                timestamp_lst.Add(timestamp);
            }
            session.insert_records_of_one_device(device_id, timestamp_lst, measurements_lst, datatype_lst, values_lst);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestInsertRecordsOfOneDevice Passed!");
        }
        public void TestCheckTimeSeriesExists(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            var ifExist_1 = session.check_time_series_exists("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1");
            var ifExist_2 = session.check_time_series_exists("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2");
            System.Diagnostics.Debug.Assert(ifExist_1 == true && ifExist_2 == false);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestCheckTimeSeriesExists Passed!");
        }
        public void TestSetTimeZone(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            session.set_time_zone("GMT+8:00");
            System.Diagnostics.Debug.Assert(session.get_time_zone() == "GMT+8:00");
            session.close();
            Console.WriteLine("TestSetTimeZone Passed!");
        }
        public void TestDeleteData(){
            var session = new Session(host, port);
            int status = 0;
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.create_time_series("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);

            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 1);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 2);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 3);
            System.Diagnostics.Debug.Assert(status == 0);
            status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 4);
            System.Diagnostics.Debug.Assert(status == 0);
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
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestDeleteData Passed!");
        }
        public void TestNonSql(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            var res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestNonSql Passed");
        }
        public void TestSqlQuery(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            session.execute_non_query_statement("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            session.execute_non_query_statement("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");

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
            res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("SELECT sql Passed");


        }
        public void TestInsertTablet(){
            var session = new Session(host, port);
            session.open(false);
            if(debug){
                session.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            string device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<string> measurement_lst = new List<string>{"TS1", "TS2", "TS3"};
            List<TSDataType> datatype_lst  = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            List<List<string>> value_lst = new List<List<string>>{new List<string>{"iotdb", true.ToString(), 12.ToString()}, new List<string>{"c#", false.ToString(), 13.ToString()}, new List<string>{"client", true.ToString(), 14.ToString()}};
            List<long> timestamp_lst = new List<long>{2, 1, 3};
            var tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = session.insert_tablet(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            // large data test
            value_lst = new List<List<string>>(){};
            timestamp_lst = new List<long>(){};
            for (int timestamp = 4; timestamp <= fetch_size * 4; timestamp++){
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<string>(){"iotdb", true.ToString(), timestamp.ToString()});
            }
            tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = session.insert_tablet(tablet);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }

            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestInsertTablet Passed!");
        }
        public void TestTestInsertTablet(){
            var session = new Session(host, port);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            string device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<string> measurement_lst = new List<string>{"TS1", "TS2", "TS3"};
            List<TSDataType> datatype_lst  = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            List<List<string>> value_lst = new List<List<string>>{new List<string>{"iotdb", true.ToString(), 12.ToString()}, new List<string>{"c#", false.ToString(), 13.ToString()}, new List<string>{"client", true.ToString(), 14.ToString()}};
            List<long> timestamp_lst = new List<long>{2, 1, 3};
            var tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = session.test_insert_tablet(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            value_lst = new List<List<string>>(){};
            timestamp_lst = new List<long>(){};
            for (int timestamp = 4; timestamp <= fetch_size * 4; timestamp++){
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<string>(){"iotdb", true.ToString(), timestamp.ToString()});
            }
            tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = session.test_insert_tablet(tablet);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == 0);
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestTestInsertTablet Passed!");
        }
        public void TestInsertTablets(){
            var session = new Session(host, port);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> device_id = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2"};
            List<List<string>> measurements_lst = new List<List<string>>(){new List<string>(){"TS1", "TS2", "TS3"}, new List<string>(){"TS1", "TS2", "TS3"}};
            List<List<TSDataType>> datatypes_lst = new List<List<TSDataType>>(){new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 }, new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 }};
            List<List<List<string>>> values_lst = new List<List<List<string>>>(){new List<List<string>>(){new List<string>{"iotdb", true.ToString(), 12.ToString()}, new List<string>{"c#", false.ToString(), 13.ToString()}, new List<string>{"client", true.ToString(), 14.ToString()}}, new List<List<string>>(){new List<string>{"iotdb_2", true.ToString(), 1.ToString()}, new List<string>{"c#_2", false.ToString(), 2.ToString()}, new List<string>{"client_2", true.ToString(), 3.ToString()}}};
            List<List<long>> timestamp_lst = new List<List<long>>(){new List<long>(){2, 1, 3}, new List<long>(){3, 1, 2}};
            List<Tablet> tablets = new List<Tablet>(){};
            for(int i = 0;i < device_id.Count; i++){
                var tablet = new Tablet(device_id[i], measurements_lst[i], datatypes_lst[i], values_lst[i], timestamp_lst[i]);
                tablets.Add(tablet);
            }
            status = session.insert_tablets(tablets);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            device_id = new List<string>(){};
            measurements_lst = new List<List<string>>(){};
            values_lst = new List<List<List<string>>>(){};
            datatypes_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<List<long>>(){};
            tablets = new List<Tablet>(){};
            for(int timestamp = 4;timestamp <= 4 * fetch_size; timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
                measurements_lst.Add(new List<string>(){"TS1", "TS2", "TS3"});
                values_lst.Add(new List<List<string>>(){new List<string>(){"iotdb", true.ToString(), timestamp.ToString()}});
                datatypes_lst.Add(new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(new List<long>(){timestamp});
                Tablet tablet = new Tablet(device_id[timestamp-4], measurements_lst[timestamp-4], datatypes_lst[timestamp-4], values_lst[timestamp-4], timestamp_lst[timestamp-4]);
                tablets.Add(tablet);
            }
            session.insert_tablets(tablets);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session.close();
            Console.WriteLine("TestInsertTablets Passed!");
        }
        public void TestTestInsertTablets(){
            var session = new Session(host, port);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> device_id = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2"};
            List<List<string>> measurements_lst = new List<List<string>>(){new List<string>(){"TS1", "TS2", "TS3"}, new List<string>(){"TS1", "TS2", "TS3"}};
            List<List<TSDataType>> datatypes_lst = new List<List<TSDataType>>(){new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 }, new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 }};
            List<List<List<string>>> values_lst = new List<List<List<string>>>(){new List<List<string>>(){new List<string>{"iotdb", true.ToString(), 12.ToString()}, new List<string>{"c#", false.ToString(), 13.ToString()}, new List<string>{"client", true.ToString(), 14.ToString()}}, new List<List<string>>(){new List<string>{"iotdb_2", true.ToString(), 1.ToString()}, new List<string>{"c#_2", false.ToString(), 2.ToString()}, new List<string>{"client_2", true.ToString(), 3.ToString()}}};
            List<List<long>> timestamp_lst = new List<List<long>>(){new List<long>(){2, 1, 3}, new List<long>(){3, 1, 2}};
            List<Tablet> tablets = new List<Tablet>(){};
            for(int i = 0;i < device_id.Count; i++){
                var tablet = new Tablet(device_id[i], measurements_lst[i], datatypes_lst[i], values_lst[i], timestamp_lst[i]);
                tablets.Add(tablet);
            }
            status = session.test_insert_tablets(tablets);
            System.Diagnostics.Debug.Assert(status == 0);
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            device_id = new List<string>(){};
            measurements_lst = new List<List<string>>(){};
            values_lst = new List<List<List<string>>>(){};
            datatypes_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<List<long>>(){};
            tablets = new List<Tablet>(){};
            for(int timestamp = 4;timestamp <= 4 * fetch_size; timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
                measurements_lst.Add(new List<string>(){"TS1", "TS2", "TS3"});
                values_lst.Add(new List<List<string>>(){new List<string>(){"iotdb", true.ToString(), timestamp.ToString()}});
                datatypes_lst.Add(new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(new List<long>(){timestamp});
                Tablet tablet = new Tablet(device_id[timestamp-4], measurements_lst[timestamp-4], datatypes_lst[timestamp-4], values_lst[timestamp-4], timestamp_lst[timestamp-4]);
                tablets.Add(tablet);
            }
            session.test_insert_tablets(tablets);
            res = session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == 0);
            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session.close();
            Console.WriteLine("TestTestInsertTablets Passed!");
        }
        void TestLargeData(){
            var session = new Session(host, port, user, passwd, fetch_size);
            session.open(false);
            System.Diagnostics.Debug.Assert(session.is_open());
            int status = 0;
            status = session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            int record_inserted_count = fetch_size * 4;
            for(var timestamp = 0; timestamp < record_inserted_count; timestamp++){
                var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"};
                var values = new List<string>{"test_record", "test_record"};
                status = session.insert_record("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, timestamp);
                System.Diagnostics.Debug.Assert(status == 0);
            }
            var res=session.execute_query_statement("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }

            session.delete_storage_group("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            System.Diagnostics.Debug.Assert(record_inserted_count == res_count);
            session.close();
            Console.WriteLine("TestTestInsertTablet Passed!");
        }
        static void Main(){
            SessionTest session_test = new SessionTest();
            
            session_test.TestOpen();
            session_test.TestClose();
            session_test.TestSetAndDeleteStorageGroup();
            session_test.TestCreateTimeSeries();
            session_test.TestDeleteTimeSeries();
            session_test.TestCreateMultiTimeSeries();
            session_test.TestDeleteStorageGroups();
            session_test.TestGetTimeZone();
            session_test.TestInsertStrRecord();
            session_test.TestInsertRecord();
            session_test.TestTestInsertRecord();
            session_test.TestTestInsertRecords();
            session_test.TestInsertRecordsOfOneDevice();
            session_test.TestCheckTimeSeriesExists();
            session_test.TestSetTimeZone();
            session_test.TestDeleteData();
            session_test.TestNonSql();
            session_test.TestSqlQuery();
            session_test.TestInsertRecords();
            session_test.TestInsertTablet();
            session_test.TestTestInsertTablet();
            session_test.TestInsertTablets();
            session_test.TestTestInsertTablets();
            session_test.TestLargeData();
            Console.WriteLine("TEST PASSED");

        }
    }

}