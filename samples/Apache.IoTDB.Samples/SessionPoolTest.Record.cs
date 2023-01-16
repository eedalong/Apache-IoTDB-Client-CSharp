using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;

namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {

        public async Task TestInsertRecord()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            int status;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);

            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.UNCOMPRESSED);

            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[3]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>
                {test_measurements[1], test_measurements[2], test_measurements[3]};
            var values = new List<object> { "test_text", true, (int)123 };
            var tasks = new List<Task<int>>();
            var start_ms = DateTime.Now.Ticks / 10000;
            for (var timestamp = 1; timestamp <= fetch_size * processed_size; timestamp++)
            {
                var rowRecord = new RowRecord(timestamp, values, measures);
                var task = session_pool.InsertRecordAsync(
                    string.Format("{0}.{1}", test_group_name, test_device), rowRecord);
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            var end_ms = DateTime.Now.Ticks / 10000;
            Console.WriteLine(string.Format("total insert aligned record time is {0}", end_ms - start_ms));
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestInsertRecordAsync Passed");
        }
        public async Task TestInsertStringRecord()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            var status = 0;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            await session_pool.DeleteStorageGroupAsync(test_group_name);

            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[0]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.UNCOMPRESSED);

            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measurements = new List<string>
                {test_measurements[0], test_measurements[1], test_measurements[2]};
            var values = new List<string> { "test_text1", "test_text2", "test_text3" };
            var tasks = new List<Task<int>>();
            var start_ms = DateTime.Now.Ticks / 10000;
            for (var timestamp = 1; timestamp <= fetch_size * processed_size; timestamp++)
            {
                var task = session_pool.InsertStringRecordAsync(
                    string.Format("{0}.{1}", test_group_name, test_device), measurements, values, timestamp);
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            var end_ms = DateTime.Now.Ticks / 10000;
            Console.WriteLine(string.Format("total insert string record time is {0}", end_ms - start_ms));
            var res = await session_pool.ExecuteQueryStatementAsync("select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            var res_cnt = 0;
            while (res.HasNext())
            {
                res.Next();
                res_cnt++;
            }
            Console.WriteLine(res_cnt + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_cnt == fetch_size * processed_size);
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestInsertStringRecordAsync Passed");
        }
        public async Task TestInsertStrRecord()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            var status = 0;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            await session_pool.DeleteStorageGroupAsync(test_group_name);

            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);

            var measures = new List<string> { test_measurements[1], test_measurements[2] };
            var values = new List<object> { (int)1, (int)2 };
            var rowRecord = new RowRecord(1, values, measures);
            status = await session_pool.InsertRecordAsync(
                string.Format("{0}.{1}", test_group_name, test_device), rowRecord);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<2");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();

            var tasks = new List<Task<int>>();
            // large data test
            var rowRecords = new List<RowRecord>() { };
            for (var timestamp = 2; timestamp <= fetch_size * processed_size; timestamp++)
                rowRecords.Add(new RowRecord(timestamp, values, measures));

            for (var timestamp = 2; timestamp <= fetch_size * processed_size; timestamp++)
            {
                var task = session_pool.InsertRecordAsync(
                    string.Format("{0}.{1}", test_group_name, test_device), rowRecords[timestamp - 2]);
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public async Task TestInsertRecords()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[3]),
                TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[4]),
                TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[5]),
                TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[6]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);

            var device_id = new List<string>() { };
            for (var i = 0; i < 3; i++) device_id.Add(string.Format("{0}.{1}", test_group_name, test_device));

            var measurements_lst = new List<List<string>>() { };
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4]
            });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4],
                test_measurements[5],
                test_measurements[6]
            });
            var values_lst = new List<List<object>>() { };
            values_lst.Add(new List<object>() { true, (int)123 });
            values_lst.Add(new List<object>() { true, (int)123, (long)456, (double)1.1 });
            values_lst.Add(new List<object>()
                {true, (int) 123, (long) 456, (double) 1.1, (float) 10001.1, "test_record"});
            var timestamp_lst = new List<long>() { 1, 2, 3 };
            var rowRecords = new List<RowRecord>() { };
            for (var i = 0; i < 3; i++)
            {
                var rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }

            status = await session_pool.InsertRecordsAsync(device_id, rowRecords);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();
            Console.WriteLine(status);

            // large data test
            device_id = new List<string>() { };
            rowRecords = new List<RowRecord>() { };
            var tasks = new List<Task<int>>();
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                device_id.Add(string.Format("{0}.{1}", test_group_name, test_device));
                rowRecords.Add(new RowRecord(timestamp, new List<object>() { true, (int)123 },
                    new List<string>() { test_measurements[1], test_measurements[2] }));
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.InsertRecordsAsync(device_id, rowRecords));
                    device_id = new List<string>() { };
                    rowRecords = new List<RowRecord>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            res.ShowTableNames();
            var record_count = fetch_size * processed_size;
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == record_count);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestInsertRecords Passed!");
        }
        public async Task TestInsertStringRecords()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);

            var device_id = new List<string>() { };
            for (var i = 0; i < 3; i++) device_id.Add(string.Format("{0}.{1}", test_group_name, test_device));

            var measurements_lst = new List<List<string>>() { };
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            var values_lst = new List<List<string>>() { };
            values_lst.Add(new List<string>() { "test1", "test2" });
            values_lst.Add(new List<string>() { "test3", "test4" });
            values_lst.Add(new List<string>() { "test5", "test6" });
            var timestamp_lst = new List<long>() { 1, 2, 3 };

            status = await session_pool.InsertStringRecordsAsync(device_id, measurements_lst, values_lst, timestamp_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();

            // large data test
            device_id = new List<string>() { };
            measurements_lst = new List<List<string>>() { };
            values_lst = new List<List<string>>() { };
            timestamp_lst = new List<long>() { };
            var tasks = new List<Task<int>>();
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                device_id.Add(string.Format("{0}.{1}", test_group_name, test_device));
                measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
                values_lst.Add(new List<string>() { "test" + timestamp, "test" + timestamp });
                timestamp_lst.Add(timestamp);
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.InsertStringRecordsAsync(device_id, measurements_lst, values_lst, timestamp_lst));
                    device_id = new List<string>() { };
                    measurements_lst = new List<List<string>>() { };
                    values_lst = new List<List<string>>() { };
                    timestamp_lst = new List<long>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            res.ShowTableNames();
            var record_count = fetch_size * processed_size;
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == record_count);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestInsertStringRecords Passed!");
        }
        public async Task TestInsertRecordsOfOneDevice()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[3]),
                TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[4]),
                TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[5]),
                TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[6]), TSDataType.TEXT,
                TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            var device_id = string.Format("{0}.{1}", test_group_name, test_device);
            var measurements_lst = new List<List<string>>() { };
            measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4]
            });
            measurements_lst.Add(new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4],
                test_measurements[5],
                test_measurements[6]
            });
            var values_lst = new List<List<object>>() { };
            values_lst.Add(new List<object>() { true, (int)123 });
            values_lst.Add(new List<object>() { true, (int)123, (long)456, (double)1.1 });
            values_lst.Add(new List<object>()
                {true, (int) 123, (long) 456, (double) 1.1, (float) 10001.1, "test_record"});
            var timestamp_lst = new List<long>() { 1, 2, 3 };
            var rowRecords = new List<RowRecord>() { };
            for (var i = 0; i < 3; i++)
            {
                var rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }

            status = await session_pool.InsertRecordsOfOneDeviceAsync(device_id, rowRecords);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();
            // large data test
            rowRecords = new List<RowRecord>() { };
            var tasks = new List<Task<int>>();
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                rowRecords.Add(new RowRecord(timestamp, new List<object>() { true, (int)123 },
                    new List<string>() { test_measurements[1], test_measurements[2] }));
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.InsertRecordsOfOneDeviceAsync(device_id, rowRecords));
                    rowRecords = new List<RowRecord>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestInsertRecordsOfOneDevice Passed!");
        }
        public async Task TestInsertStringRecordsOfOneDevice()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            var status = 0;
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[0]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);

            var device_id = string.Format("{0}.{1}", test_group_name, test_device);
            var measurements_lst = new List<List<string>>() { };
            measurements_lst.Add(new List<string>() { test_measurements[0], test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>() { test_measurements[0], test_measurements[1], test_measurements[2] });
            measurements_lst.Add(new List<string>() { test_measurements[0], test_measurements[1], test_measurements[2] });

            var values_lst = new List<List<string>>() { };
            values_lst.Add(new List<string>() { "test1", "test2", "test3" });
            values_lst.Add(new List<string>() { "test4", "test5", "test6" });
            values_lst.Add(new List<string>() { "test7", "test8", "test9" });

            var timestamp_lst = new List<long>() { 1, 2, 3 };

            status = await session_pool.InsertStringRecordsOfOneDeviceAsync(device_id, timestamp_lst, measurements_lst, values_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();
            // large data test
            values_lst = new List<List<string>>() { };
            var tasks = new List<Task<int>>();
            measurements_lst = new List<List<string>>() { };
            timestamp_lst = new List<long>() { };
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                values_lst.Add(new List<string>() { "test1", "test2" });
                measurements_lst.Add(new List<string>() { test_measurements[1], test_measurements[2] });
                timestamp_lst.Add(timestamp);
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.InsertStringRecordsOfOneDeviceAsync(device_id, timestamp_lst, measurements_lst, values_lst));
                    values_lst = new List<List<string>>() { };
                    measurements_lst = new List<List<string>>() { };
                    timestamp_lst = new List<long>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            Console.WriteLine(res_count + " " + fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestInsertStringRecordsOfOneDevice Passed!");
        }
    }
}