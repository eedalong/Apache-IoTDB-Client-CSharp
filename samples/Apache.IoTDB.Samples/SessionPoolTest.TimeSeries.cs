
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
namespace Apache.IoTDB.Samples
{
    public partial class SessionPoolTest
    {
        public async Task TestCreateMultiTimeSeries()
        {
            // by Luzhan
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            await session_pool.Open(false);
            var status = 0;
            if (debug) session_pool.OpenDebugMode();

            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            var measurement_lst = new List<int> { 1, 2, 3, 4, 5, 6 };
            var ts_path_lst = new List<string>(measurement_lst.ConvertAll(
                (measurement) => string.Format("{0}.{1}.{2}{3}", test_group_name, test_device, test_measurement, measurement)));
            var data_type_lst = new List<TSDataType>()
            {
                TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                TSDataType.TEXT
            };
            var encoding_lst = new List<TSEncoding>()
            {
                TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
                TSEncoding.PLAIN
            };
            var compressor_lst = new List<Compressor>()
            {
                Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY,
                Compressor.SNAPPY
            };
            status = await session_pool.CreateMultiTimeSeriesAsync(ts_path_lst, data_type_lst, encoding_lst,
                compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestCreateMultiTimeSeries Passed!");
        }

        public async Task TestDeleteTimeSeries()
        {
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            await session_pool.Open(false);
            var status = 0;
            if (debug) session_pool.OpenDebugMode();

            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            var measurement_lst = new List<int> { 1, 2, 3, 4, 5, 6 };
            var ts_path_lst = new List<string>(measurement_lst.ConvertAll(
                (measurement) => string.Format("{0}.{1}.{2}{3}", test_group_name, test_device, test_measurement, measurement)));
            var data_type_lst = new List<TSDataType>()
            {
                TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                TSDataType.TEXT
            };
            var encoding_lst = new List<TSEncoding>()
            {
                TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
                TSEncoding.PLAIN
            };
            var compressor_lst = new List<Compressor>()
            {
                Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY,
                Compressor.SNAPPY
            };
            status = await session_pool.CreateMultiTimeSeriesAsync(ts_path_lst, data_type_lst, encoding_lst,
                compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteTimeSeriesAsync(ts_path_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            Console.WriteLine("TestDeleteTimeSeries Passed!");
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
        }
        public async Task TestCreateTimeSeries()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]),
                TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[3]),
                TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[4]),
                TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[5]),
                TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            System.Diagnostics.Debug.Assert(await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[6]),
                TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY) == 0);
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.Close();
            Console.WriteLine("TestCreateTimeSeries Passed!");
        }

        public async Task TestCreateAlignedTimeseries()
        {
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            await session_pool.Open(false);
            var status = 0;
            if (debug) session_pool.OpenDebugMode();

            status = await session_pool.DeleteStorageGroupAsync(test_group_name);

            string prefixPath = string.Format("{0}.{1}", test_group_name, test_device);
            var measurement_lst = new List<string>()
            {
                test_measurements[1],
                test_measurements[2],
                test_measurements[3],
                test_measurements[4],
                test_measurements[5],
                test_measurements[6]
            };
            var data_type_lst = new List<TSDataType>()
            {
                TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                TSDataType.TEXT
            };
            var encoding_lst = new List<TSEncoding>()
            {
                TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.PLAIN,
                TSEncoding.PLAIN
            };
            var compressor_lst = new List<Compressor>()
            {
                Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY, Compressor.SNAPPY,
                Compressor.SNAPPY
            };
            status = await session_pool.CreateAlignedTimeseriesAsync(prefixPath, measurement_lst, data_type_lst, encoding_lst,
                compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestCreateAlignedTimeSeries Passed!");
        }
        public async Task TestCheckTimeSeriesExists()
        {
            var session_pool = new SessionPool(host, port, pool_size);
            var status = 0;
            await session_pool.Open(false);
            if (debug) session_pool.OpenDebugMode();

            System.Diagnostics.Debug.Assert(session_pool.IsOpen());
            await session_pool.DeleteStorageGroupAsync(test_group_name);
            await session_pool.CreateTimeSeries(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]),
                TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            var ifExist_1 = await session_pool.CheckTimeSeriesExistsAsync(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[1]));
            var ifExist_2 = await session_pool.CheckTimeSeriesExistsAsync(
                string.Format("{0}.{1}.{2}", test_group_name, test_device, test_measurements[2]));
            System.Diagnostics.Debug.Assert(ifExist_1 == true && ifExist_2 == false);
            status = await session_pool.DeleteStorageGroupAsync(test_group_name);
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.Close();
            Console.WriteLine("TestCheckTimeSeriesExists Passed!");
        }
    }

}