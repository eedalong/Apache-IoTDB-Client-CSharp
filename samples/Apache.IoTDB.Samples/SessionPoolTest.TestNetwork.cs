using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {
        public async Task TestTestInsertRecord()
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
                {
                    test_measurements[1], test_measurements[2], test_measurements[3]
                };
            var values = new List<object> { "test_text", true, (int)123 };
            var tasks = new List<Task<int>>();
            var start_ms = DateTime.Now.Ticks / 10000;
            for (var timestamp = 1; timestamp <= fetch_size * processed_size; timestamp++)
            {
                var rowRecord = new RowRecord(timestamp, values, measures);
                var task = session_pool.TestInsertRecordAsync(
                    string.Format("{0}.{1}", test_group_name, test_device), rowRecord);
                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray());
            var end_ms = DateTime.Now.Ticks / 10000;
            Console.WriteLine(string.Format("total insert record time is {0}", end_ms - start_ms));
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestTestInsertRecordAsync Passed");
        }

        public async Task TestTestInsertRecords()
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

            status = await session_pool.TestInsertRecordsAsync(device_id, rowRecords);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<10");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();

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
                    tasks.Add(session_pool.TestInsertRecordsAsync(device_id, rowRecords));
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
            System.Diagnostics.Debug.Assert(res_count == 0);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestTestInsertRecords Passed!");
        }

        public async Task TestTestInsertTablet()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            var status = 0;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            var device_id = string.Format("{0}.{1}", test_group_name, test_device);
            var measurement_lst = new List<string>
                {   test_measurements[1],
                    test_measurements[2],
                    test_measurements[3]
                };
            var value_lst = new List<List<object>>
            {
                new() {"iotdb", true, (int) 12}, new() {"c#", false, (int) 13},
                new() {"client", true, (int) 14}
            };
            var timestamp_lst = new List<long> { 2, 1, 3 };
            var datatype_lst = new List<TSDataType> { TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 };
            var tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
            status = await session_pool.TestInsertTabletAsync(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device) + " where time<15");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());

            await res.Close();
            // large data test
            value_lst = new List<List<object>>() { };
            timestamp_lst = new List<long>() { };
            var tasks = new List<Task<int>>();
            var start_ms = DateTime.Now.Ticks / 10000;
            for (var timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++)
            {
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<object>() { "iotdb", true, (int)timestamp });
                if (timestamp % (fetch_size / 32) == 0)
                {
                    tablet = new Tablet(device_id, measurement_lst, datatype_lst, value_lst, timestamp_lst);
                    tasks.Add(session_pool.TestInsertTabletAsync(tablet));
                    value_lst = new List<List<object>>() { };
                    timestamp_lst = new List<long>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            var end_ms = DateTime.Now.Ticks / 10000;
            Console.WriteLine(string.Format("total tablet insert time is {0}", end_ms - start_ms));
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_device));
            res.ShowTableNames();
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestTestInsertTablet Passed!");
        }

        public async Task TestTestInsertTablets()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            var status = 0;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            var device_id = new List<string>()
            {
                string.Format("{0}.{1}", test_group_name, test_devices[1]),
                string.Format("{0}.{1}", test_group_name, test_devices[2])
            };
            var measurements_lst = new List<List<string>>()
            {
                new() {test_measurements[1], test_measurements[2], test_measurements[3]},
                new() {test_measurements[1], test_measurements[2], test_measurements[3]}
            };
            var values_lst = new List<List<List<object>>>()
            {
                new()
                {
                    new List<object>() {"iotdb", true, (int) 12}, new List<object>() {"c#", false, (int) 13},
                    new List<object>() {"client", true, (int) 14}
                },
                new()
                {
                    new List<object>() {"iotdb_2", true, (int) 1}, new List<object>() {"c#_2", false, (int) 2},
                    new List<object>() {"client_2", true, (int) 3}
                }
            };
            var datatype_lst = new List<List<TSDataType>>()
            {
                new() {TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32},
                new() {TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32}
            };
            var timestamp_lst = new List<List<long>>()
                {new() {2, 1, 3}, new() {3, 1, 2}};
            var tablets = new List<Tablet>() { };
            for (var i = 0; i < device_id.Count; i++)
            {
                var tablet = new Tablet(device_id[i], measurements_lst[i], datatype_lst[i], values_lst[i], timestamp_lst[i]);
                tablets.Add(tablet);
            }

            status = await session_pool.TestInsertTabletsAsync(tablets);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_devices[1]) + " where time<15");
            res.ShowTableNames();
            while (res.HasNext()) Console.WriteLine(res.Next());
            await res.Close();

            // large data test

            var tasks = new List<Task<int>>();
            for (var timestamp = 4; timestamp <= processed_size * fetch_size; timestamp++)
            {
                var local_device_id = string.Format("{0}.{1}", test_group_name, test_devices[1]);
                var local_measurements = new List<string>()
                    {test_measurements[1], test_measurements[2], test_measurements[3]};
                var local_value = new List<List<object>>() { new() { "iotdb", true, (int)timestamp } };
                var local_timestamp = new List<long> { timestamp };
                var local_data_type = new List<TSDataType> { TSDataType.TEXT, TSDataType.BOOLEAN, TSDataType.INT32 };
                var tablet = new Tablet(local_device_id, local_measurements, local_data_type, local_value, local_timestamp);
                tablets.Add(tablet);
                if (timestamp % fetch_size == 0)
                {
                    tasks.Add(session_pool.TestInsertTabletsAsync(tablets));
                    tablets = new List<Tablet>() { };
                }
            }

            Task.WaitAll(tasks.ToArray());
            res = await session_pool.ExecuteQueryStatementAsync(
                "select * from " + string.Format("{0}.{1}", test_group_name, test_devices[1]));
            res.ShowTableNames();
            var res_count = 0;
            while (res.HasNext())
            {
                res.Next();
                res_count += 1;
            }

            await res.Close();
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestTestInsertTablets Passed!");
        }

    }
}