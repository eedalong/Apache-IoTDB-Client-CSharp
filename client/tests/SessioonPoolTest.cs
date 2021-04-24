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
        public string host = "81.69.18.71";
        public int port = 8888;
        public string user = "root";
        public string passwd = "root";
        public int fetch_size = 5000;
        public bool debug = false;
        int pool_size = 50;

        public void Test(){
            var task = TestInsertRecord();
            task.Wait();
            task = TestCreateMultiTimeSeries();
            task.Wait();
            task = TestGetTimeZone();
            task.Wait();
            task = TestInsertStrRecord();
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
        
        
    }
}