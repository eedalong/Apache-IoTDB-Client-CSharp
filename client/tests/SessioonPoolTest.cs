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
        public int fetch_size = 5000;
        public bool debug = false;

        public void Test(){
            TestInsertRecordAsync();
        }

        void TestInsertRecordAsync(){
            var session_pool = new SessionPool(host, port, 100);
            session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            var task = session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            task.Wait();

            task = session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            task.Wait();
            System.Diagnostics.Debug.Assert(task.Result== 0);
            task = session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            task.Wait();
            System.Diagnostics.Debug.Assert(task.Result== 0);
            task = session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            task.Wait();
            System.Diagnostics.Debug.Assert(task.Result == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<string>{"test_text", true.ToString(), 123.ToString()};
            var types = new List<TSDataType>{TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32};
            List<Task<int>> tasks = new List<Task<int>>();
            for(int timestamp = 1; timestamp <= fetch_size * 4; timestamp++){
                task = session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", measures, values, types, timestamp);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
            Console.WriteLine("TestInsertRecordAsync Passed");
        }
        
    }
}