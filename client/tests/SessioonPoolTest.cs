using System.Collections.Generic;
using System;
using iotdb_client_csharp.client.utils;
using Thrift;
using System.Threading.Tasks;
using System.Threading;

namespace iotdb_client_csharp.client.test
{
    public class SessionPoolTest
    {
        public string host = "localhost";
        public int port = 6667;
        public string user = "root";
        public string passwd = "root";
        public int fetch_size = 40000;
        public bool debug = false;
        int pool_size = 20;

        public void Test(){
            var task = TestInsertRecord();
            task.Wait();
            task = TestCreateMultiTimeSeries();
            task.Wait();
            task = TestGetTimeZone();
            task.Wait();
            task = TestInsertStrRecord();
            task.Wait();
            task = TestInsertRecords();
            task.Wait();
            task = TestInsertRecordsOfOneDevice();
            task.Wait();
            task = TestInsertTablet();
            task.Wait();
            task = TestInsertTablets();
            task.Wait();
            task = TestSetAndDeleteStorageGroup();
            task.Wait();
            task = TestCreateTimeSeries();
            task.Wait();
            task = TestDeleteStorageGroups();
            task.Wait();
            task = TestCheckTimeSeriesExists();
            task.Wait();
            task = TestSetTimeZone();
            task.Wait();
            task = TestDeleteData();
            task.Wait();
            task = TestNonSql();
            task.Wait();
            task = TestSqlQuery();
            task.Wait();
            
        }

        public async Task TestInsertRecord(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");

            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
          
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status== 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            List<Task<int>> tasks = new List<Task<int>>();
            for(int timestamp = 1; timestamp <= fetch_size * 4; timestamp++){
                var task = session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, timestamp);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session_pool.close();
            Console.WriteLine("TestInsertRecordAsync Passed");
        }
        public async Task TestCreateMultiTimeSeries(){
            // by Luzhan
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            session_pool.open(false);
            int status = 0;
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            status = await session_pool.create_multi_time_series_async(ts_path_lst, data_type_lst, encoding_lst, compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();      
            Console.WriteLine("TestCreateMultiTimeSeries Passed!");
        }
        public async Task TestDeleteTimeSeries(){
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            session_pool.open(false);
            int status = 0;
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            status = await session_pool.create_multi_time_series_async(ts_path_lst, data_type_lst, encoding_lst, compressor_lst);
            System.Diagnostics.Debug.Assert(status==0);
            status = await session_pool.delete_storage_groups_async(ts_path_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            Console.WriteLine("TestDeleteTimeSeries Passed!");
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session_pool.close();            
        }
        public async Task TestGetTimeZone(){
           var session_pool = new SessionPool(host, port, pool_size);
           session_pool.open(false);
           if(debug){
                session_pool.open_debug_mode();
            }
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
           System.Diagnostics.Debug.Assert(session_pool.is_open());
           var time_zone = session_pool.get_time_zone();
           System.Diagnostics.Debug.Assert(time_zone == "UTC+08:00");
           session_pool.close();
           Console.WriteLine("TestGetTimeZone Passed!");
        }
         public async Task TestInsertStrRecord(){
           var session_pool = new SessionPool(host, port, pool_size);
           int status = 0;
           session_pool.open(false);
           if(debug){
                session_pool.open_debug_mode();
            }
           System.Diagnostics.Debug.Assert(session_pool.is_open());
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");

           status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);

           var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"};
           var values = new List<string>{"test_record", "test_record"};
           status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, 1);
           System.Diagnostics.Debug.Assert(status == 0);

           var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<2");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }

            var tasks = new List<Task<int>>();
           // large data test
           for(int timestamp = 2; timestamp <=fetch_size * 4; timestamp++){
               var task = session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, timestamp);
               tasks.Add(task);
           }
           Task.WaitAll(tasks.ToArray());
           res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
           int res_count = 0;
           while(res.has_next()){
               res.next();
               res_count += 1;
           }
           System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
           session_pool.close();
           Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public async Task TestInsertRecords(){
            var session_pool = new SessionPool(host, port, pool_size);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            int status = 0;
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
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
            status = await session_pool.insert_records_async(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
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
            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= fetch_size * 4;timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
                values_lst.Add(new List<string>(){"true", 123.ToString()});
                measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
                datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(timestamp);
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_records_async(device_id, measurements_lst, values_lst, datatype_lst, timestamp_lst));
                    device_id = new List<string>(){};
                    values_lst = new List<List<string>>(){};
                    measurements_lst = new List<List<string>>(){};
                    datatype_lst = new List<List<TSDataType>>(){};
                    timestamp_lst = new List<long>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int record_count = fetch_size * 4;
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == record_count);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestInsertRecords Passed!");
        }
        public async Task TestInsertRecordsOfOneDevice(){
            var session_pool = new SessionPool(host, port, pool_size);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            int status = 0;
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
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
            status = await session_pool.insert_records_of_one_device_async(device_id, timestamp_lst, measurements_lst, datatype_lst, values_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }

            // large data test
            measurements_lst = new List<List<string>>(){};
            values_lst = new List<List<string>>(){};
            datatype_lst = new List<List<TSDataType>>(){};
            timestamp_lst = new List<long>(){};
            var tasks = new List<Task<int>>();
            for(int timestamp = 4; timestamp <= fetch_size * 4;timestamp++){
                measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
                values_lst.Add(new List<string>(){"true", 123.ToString(), 456.ToString(), 1.1.ToString(), 10001.1.ToString(), "test_record"});
                datatype_lst.Add(new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.TEXT});
                timestamp_lst.Add(timestamp);
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_records_of_one_device_async(device_id, timestamp_lst, measurements_lst, datatype_lst, values_lst));
                    measurements_lst = new List<List<string>>(){};
                    values_lst = new List<List<string>>(){};
                    datatype_lst = new List<List<TSDataType>>(){};
                    timestamp_lst = new List<long>(){};

                }

            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestInsertRecordsOfOneDevice Passed!");
        }
        public async Task TestInsertTablet(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            string device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<string> measurement_lst = new List<string>{"TS1", "TS2", "TS3"};
            List<TSDataType> datatype_lst  = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            List<List<string>> value_lst = new List<List<string>>{new List<string>{"iotdb", true.ToString(), 12.ToString()}, new List<string>{"c#", false.ToString(), 13.ToString()}, new List<string>{"client", true.ToString(), 14.ToString()}};
            List<long> timestamp_lst = new List<long>{2, 1, 3};
            var tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = await session_pool.insert_tablet_async(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            // large data test
            value_lst = new List<List<string>>(){};
            timestamp_lst = new List<long>(){};
            var tasks = new List<Task<int>>();
            for (int timestamp = 4; timestamp <= fetch_size * 4; timestamp++){
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<string>(){"iotdb", true.ToString(), timestamp.ToString()});
                if(timestamp % fetch_size == 0){
                    tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
                    tasks.Add(session_pool.insert_tablet_async(tablet));
                    value_lst = new List<List<string>>(){};
                    timestamp_lst = new List<long>(){};

                }
            }
            long start_ms= (DateTime.Now.Ticks / 10000);
            Task.WaitAll(tasks.ToArray());
            long end_ms = (DateTime.Now.Ticks / 10000);
            Console.WriteLine(string.Format("total tablet insert time is {0}", end_ms - start_ms));
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }

            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestInsertTablet Passed!");
        }
        public async Task TestInsertTablets(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
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
            status = await session_pool.insert_tablets_async(tablets);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2 where time<15");
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
            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= 4 * fetch_size; timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
                measurements_lst.Add(new List<string>(){"TS1", "TS2", "TS3"});
                values_lst.Add(new List<List<string>>(){new List<string>(){"iotdb", true.ToString(), timestamp.ToString()}});
                datatypes_lst.Add(new List<TSDataType>(){TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32});
                timestamp_lst.Add(new List<long>(){timestamp});
                Tablet tablet = new Tablet(device_id[timestamp-4], measurements_lst[timestamp-4], datatypes_lst[timestamp-4], values_lst[timestamp-4], timestamp_lst[timestamp-4]);
                tablets.Add(tablet);
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_tablets_async(tablets));
                    device_id = new List<string>(){};
                    measurements_lst = new List<List<string>>(){};
                    values_lst = new List<List<List<string>>>(){};
                    datatypes_lst = new List<List<TSDataType>>(){};
                    timestamp_lst = new List<List<long>>(){};
                    tablets = new List<Tablet>(){};

                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            System.Diagnostics.Debug.Assert(res_count == fetch_size * 4);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestInsertTablets Passed!");
        }
        public async Task TestSetAndDeleteStorageGroup(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            System.Diagnostics.Debug.Assert(await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            session_pool.close();
            Console.WriteLine("TestSetAndDeleteStorageGroup Passed!");
        }
        public async Task TestCreateTimeSeries(){
            var session_pool = new SessionPool(host, port, pool_size);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1"), TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"), TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3"), TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4"), TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5"), TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"), TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            session_pool.close();
            Console.WriteLine("TestCreateTimeSeries Passed!");
        }
        public async Task TestDeleteStorageGroups(){
            var session_pool = new SessionPool(host, port, pool_size);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
           
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            List<string> group_names = new List<string>(){};
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            System.Diagnostics.Debug.Assert(await session_pool.delete_storage_groups_async(group_names)==0);
            session_pool.close();
            Console.WriteLine("TestDeleteStorageGroups Passed!");
        }
        public async Task TestCheckTimeSeriesExists(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            var ifExist_1 = await session_pool.check_time_series_exists_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1");
            var ifExist_2 = await session_pool.check_time_series_exists_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2");
            System.Diagnostics.Debug.Assert(ifExist_1 == true && ifExist_2 == false);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestCheckTimeSeriesExists Passed!");
        }
        public async Task TestSetTimeZone(){
            var session_pool = new SessionPool(host, port, pool_size);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            session_pool.set_time_zone("GMT+8:00");
            System.Diagnostics.Debug.Assert(session_pool.get_time_zone() == "GMT+8:00");
            session_pool.close();
            Console.WriteLine("TestSetTimeZone Passed!");
        }
        public async Task TestDeleteData(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);

            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 1);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 2);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 3);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, 4);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"};
            await session_pool.delete_data_async(ts_path_lst, 2 ,3);
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestDeleteData Passed!");
        }
        public async Task TestNonSql(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("TestNonSql Passed");
        }
        public async Task TestSqlQuery(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");

            var res =await session_pool.execute_query_statement_async("show timeseries root");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine("SHOW TIMESERIES ROOT sql passed!");
            res =await session_pool.execute_query_statement_async("show devices");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine("SHOW DEVICES sql passed!");
            res = await session_pool.execute_query_statement_async("COUNT TIMESERIES root");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine("COUNT TIMESERIES root sql Passed");
            res= await session_pool.execute_query_statement_async("select * from root.ln.wf01 where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            Console.WriteLine("SELECT sql Passed");
            res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            session_pool.close();
            Console.WriteLine("SELECT sql Passed");
        }

    }
}