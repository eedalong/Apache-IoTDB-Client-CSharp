using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.IoTDB
{

    public class SessionPool : IDisposable
    {
        private static int SuccessCode => 200;
        private static int RedirectRecommendCode => 400;
        private static readonly TSProtocolVersion ProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

        private readonly string _username;
        private readonly string _password;
        private bool _enableRpcCompression;
        private string _zoneId;
        private readonly string _host;
        private readonly int _port;
        private readonly int _fetchSize;
        private readonly int _timeout;
        private readonly int _poolSize = 4;
        private readonly Utils _utilFunctions = new Utils();
        private bool _debugMode;
        private bool _isClose = true;
        private ConcurrentClientQueue _clients;
        private ILogger _logger;

        public SessionPool(string host, int port, int poolSize)
                        : this(host, port, "root", "root", 1024, "UTC+08:00", poolSize, true, 60)
        {
        }

        public SessionPool(string host, int port, string username, string password)
                        : this(host, port, username, password, 1024, "UTC+08:00", 8, true, 60)
        {
        }

        public SessionPool(string host, int port, string username, string password, int fetchSize)
                        : this(host, port, username, password, fetchSize, "UTC+08:00", 8, true, 60)
        {

        }

        public SessionPool(string host, int port) : this(host, port, "root", "root", 1024, "UTC+08:00", 8, true, 60)
        {
        }
        public SessionPool(string host, int port, string username, string password, int fetchSize, string zoneId, int poolSize, bool enableRpcCompression, int timeout)
        {
            _host = host;
            _port = port;
            _username = username;
            _password = password;
            _zoneId = zoneId;
            _fetchSize = fetchSize;
            _debugMode = false;
            _poolSize = poolSize;
            _enableRpcCompression = enableRpcCompression;
            _timeout = timeout;
        }

        /// <summary>
        ///   Gets or sets the amount of time a Session will wait for  a send operation to complete successfully.
        /// </summary>
        /// <remarks> The send time-out value, in milliseconds. The default is 10000.</remarks>
        public int TimeOut { get; set; } = 10000;

        ILoggerFactory factory;
        private bool disposedValue;

        public void OpenDebugMode(Action<ILoggingBuilder> configure)
        {
            _debugMode = true;
            factory = LoggerFactory.Create(configure);
            _logger = factory.CreateLogger(nameof(Apache.IoTDB));
        }

        public void CloseDebugMode()
        {
            _debugMode = false;
        }

        public async Task Open(bool enableRpcCompression, CancellationToken cancellationToken = default)
        {
            _enableRpcCompression = enableRpcCompression;
            await Open(cancellationToken);
        }

        public async Task Open(CancellationToken cancellationToken = default)
        {
            _clients = new ConcurrentClientQueue();
            _clients.Timeout = _timeout * 5;
            for (var index = 0; index < _poolSize; index++)
            {
                _clients.Add(await CreateAndOpen(_enableRpcCompression, _timeout, cancellationToken));
            }
        }

        public bool IsOpen() => !_isClose;

        public async Task Close()
        {
            if (_isClose)
            {
                return;
            }

            foreach (var client in _clients.ClientQueue.AsEnumerable())
            {
                var closeSessionRequest = new TSCloseSessionReq(client.SessionId);
                try
                {
                    await client.ServiceClient.closeSessionAsync(closeSessionRequest);
                }
                catch (TException e)
                {
                    throw new TException("Error occurs when closing session at server. Maybe server is down", e);
                }
                finally
                {
                    _isClose = true;

                    client.Transport?.Close();
                }
            }
        }

        public async Task SetTimeZone(string zoneId)
        {
            _zoneId = zoneId;

            foreach (var client in _clients.ClientQueue.AsEnumerable())
            {
                var req = new TSSetTimeZoneReq(client.SessionId, zoneId);
                try
                {
                    var resp = await client.ServiceClient.setTimeZoneAsync(req);
                    if (_debugMode)
                    {
                        _logger.LogInformation("setting time zone_id as {0}, server message:{1}", zoneId, resp.Message);
                    }
                }
                catch (TException e)
                {
                    throw new TException("could not set time zone", e);
                }
            }
        }

        public async Task<string> GetTimeZone()
        {
            if (_zoneId != "")
            {
                return _zoneId;
            }

            var client = _clients.Take();

            try
            {
                var response = await client.ServiceClient.getTimeZoneAsync(client.SessionId);

                return response?.TimeZone;
            }
            catch (TException e)
            {
                throw new TException("could not get time zone", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        private async Task<Client> CreateAndOpen(bool enableRpcCompression, int timeout, CancellationToken cancellationToken = default)
        {
            var tcpClient = new TcpClient(_host, _port);
            tcpClient.SendTimeout = timeout;
            tcpClient.ReceiveTimeout = timeout;
            var transport = new TFramedTransport(new TSocketTransport(tcpClient, null));

            if (!transport.IsOpen)
            {
                await transport.OpenAsync(cancellationToken);
            }

            var client = enableRpcCompression ?
                new IClientRPCService.Client(new TCompactProtocol(transport)) :
                new IClientRPCService.Client(new TBinaryProtocol(transport));

            var openReq = new TSOpenSessionReq(ProtocolVersion, _zoneId, _username)
            {
                Password = _password,
            };

            try
            {
                var openResp = await client.openSessionAsync(openReq, cancellationToken);

                if (openResp.ServerProtocolVersion != ProtocolVersion)
                {
                    throw new TException($"Protocol Differ, Client version is {ProtocolVersion} but Server version is {openResp.ServerProtocolVersion}", null);
                }

                if (openResp.ServerProtocolVersion == 0)
                {
                    throw new TException("Protocol not supported", null);
                }

                var sessionId = openResp.SessionId;
                var statementId = await client.requestStatementIdAsync(sessionId, cancellationToken);

                _isClose = false;

                var returnClient = new Client(
                    client,
                    sessionId,
                    statementId,
                    transport);

                return returnClient;
            }
            catch (Exception)
            {
                transport.Close();

                throw;
            }
        }

        public async Task<int> SetStorageGroup(string groupName)
        {
            var client = _clients.Take();

            try
            {
                var status = await client.ServiceClient.setStorageGroupAsync(client.SessionId, groupName);

                if (_debugMode)
                {
                    _logger.LogInformation("set storage group {0} successfully, server message is {1}", groupName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
            }
            catch (TException e)
            {
                // try to reconnect
                await Open(_enableRpcCompression);
                client = _clients.Take();
                try
                {
                    var status = await client.ServiceClient.setStorageGroupAsync(client.SessionId, groupName);
                    if (_debugMode)
                    {
                        _logger.LogInformation("set storage group {0} successfully, server message is {1}", groupName, status.Message);
                    }
                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when setting storage group", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> CreateTimeSeries(
            string tsPath,
            TSDataType dataType,
            TSEncoding encoding,
            Compressor compressor)
        {
            var client = _clients.Take();
            var req = new TSCreateTimeseriesReq(
                client.SessionId,
                tsPath,
                (int)dataType,
                (int)encoding,
                (int)compressor);
            try
            {
                var status = await client.ServiceClient.createTimeseriesAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("creating time series {0} successfully, server message is {1}", tsPath, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.createTimeseriesAsync(req);
                    if (_debugMode)
                    {
                        _logger.LogInformation("creating time series {0} successfully, server message is {1}", tsPath, status.Message);
                    }
                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when creating time series", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }

        }

        public async Task<int> CreateAlignedTimeseriesAsync(
            string prefixPath,
            List<string> measurements,
            List<TSDataType> dataTypeLst,
            List<TSEncoding> encodingLst,
            List<Compressor> compressorLst)
        {
            var client = _clients.Take();
            var dataTypes = dataTypeLst.ConvertAll(x => (int)x);
            var encodings = encodingLst.ConvertAll(x => (int)x);
            var compressors = compressorLst.ConvertAll(x => (int)x);

            var req = new TSCreateAlignedTimeseriesReq(
                client.SessionId,
                prefixPath,
                measurements,
                dataTypes,
                encodings,
                compressors);
            try
            {
                var status = await client.ServiceClient.createAlignedTimeseriesAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("creating aligned time series {0} successfully, server message is {1}", prefixPath, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.createAlignedTimeseriesAsync(req);
                    if (_debugMode)
                    {
                        _logger.LogInformation("creating aligned time series {0} successfully, server message is {1}", prefixPath, status.Message);
                    }
                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when creating aligned time series", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteStorageGroupAsync(string groupName)
        {
            var client = _clients.Take();
            try
            {
                var status = await client.ServiceClient.deleteStorageGroupsAsync(
                    client.SessionId,
                    new List<string> { groupName });

                if (_debugMode)
                {
                    _logger.LogInformation($"delete storage group {groupName} successfully, server message is {status?.Message}");
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                try
                {
                    var status = await client.ServiceClient.deleteStorageGroupsAsync(
                        client.SessionId,
                        new List<string> { groupName });

                    if (_debugMode)
                    {
                        _logger.LogInformation($"delete storage group {groupName} successfully, server message is {status?.Message}");
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when deleting storage group", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteStorageGroupsAsync(List<string> groupNames)
        {
            var client = _clients.Take();

            try
            {
                var status = await client.ServiceClient.deleteStorageGroupsAsync(client.SessionId, groupNames);

                if (_debugMode)
                {
                    _logger.LogInformation(
                        "delete storage group(s) {0} successfully, server message is {1}",
                        groupNames,
                        status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                try
                {
                    var status = await client.ServiceClient.deleteStorageGroupsAsync(client.SessionId, groupNames);

                    if (_debugMode)
                    {
                        _logger.LogInformation(
                            "delete storage group(s) {0} successfully, server message is {1}",
                            groupNames,
                            status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when deleting storage group(s)", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> CreateMultiTimeSeriesAsync(
            List<string> tsPathLst,
            List<TSDataType> dataTypeLst,
            List<TSEncoding> encodingLst,
            List<Compressor> compressorLst)
        {
            var client = _clients.Take();
            var dataTypes = dataTypeLst.ConvertAll(x => (int)x);
            var encodings = encodingLst.ConvertAll(x => (int)x);
            var compressors = compressorLst.ConvertAll(x => (int)x);

            var req = new TSCreateMultiTimeseriesReq(client.SessionId, tsPathLst, dataTypes, encodings, compressors);

            try
            {
                var status = await client.ServiceClient.createMultiTimeseriesAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("creating multiple time series {0}, server message is {1}", tsPathLst, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.createMultiTimeseriesAsync(req);
                    if (_debugMode)
                    {
                        _logger.LogInformation("creating multiple time series {0}, server message is {1}", tsPathLst, status.Message);
                    }
                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when creating multiple time series", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteTimeSeriesAsync(List<string> pathList)
        {
            var client = _clients.Take();

            try
            {
                var status = await client.ServiceClient.deleteTimeseriesAsync(client.SessionId, pathList);

                if (_debugMode)
                {
                    _logger.LogInformation("deleting multiple time series {0}, server message is {1}", pathList, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                try
                {
                    var status = await client.ServiceClient.deleteTimeseriesAsync(client.SessionId, pathList);

                    if (_debugMode)
                    {
                        _logger.LogInformation("deleting multiple time series {0}, server message is {1}", pathList, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when deleting multiple time series", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteTimeSeriesAsync(string tsPath)
        {
            return await DeleteTimeSeriesAsync(new List<string> { tsPath });
        }

        public async Task<bool> CheckTimeSeriesExistsAsync(string tsPath)
        {
            // TBD by dalong
            try
            {
                var sql = "SHOW TIMESERIES " + tsPath;
                var sessionDataset = await ExecuteQueryStatementAsync(sql);
                return sessionDataset.HasNext();
            }
            catch (TException e)
            {
                throw new TException("could not check if certain time series exists", e);
            }
        }

        public async Task<int> DeleteDataAsync(List<string> tsPathLst, long startTime, long endTime)
        {
            var client = _clients.Take();
            var req = new TSDeleteDataReq(client.SessionId, tsPathLst, startTime, endTime);

            try
            {
                var status = await client.ServiceClient.deleteDataAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation(
                        "delete data from {0}, server message is {1}",
                        tsPathLst,
                        status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.deleteDataAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation(
                            "delete data from {0}, server message is {1}",
                            tsPathLst,
                            status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when deleting data", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertRecordAsync(string deviceId, RowRecord record)
        {
            // TBD by Luzhan
            var client = _clients.Take();
            var req = new TSInsertRecordReq(client.SessionId, deviceId, record.Measurements, record.ToBytes(),
                record.Timestamps);
            try
            {
                var status = await client.ServiceClient.insertRecordAsync(req);
                if (_debugMode)
                {
                    _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting record", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedRecordAsync(string deviceId, RowRecord record)
        {
            var client = _clients.Take();
            var req = new TSInsertRecordReq(client.SessionId, deviceId, record.Measurements, record.ToBytes(),
                record.Timestamps);
            req.IsAligned = true;
            // ASSERT that the insert plan is aligned    
            System.Diagnostics.Debug.Assert(req.IsAligned == true);
            try
            {
                var status = await client.ServiceClient.insertRecordAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting record", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public TSInsertStringRecordReq GenInsertStrRecordReq(string deviceId, List<string> measurements,
            List<string> values, long timestamp, long sessionId, bool isAligned = false)
        {
            if (values.Count() != measurements.Count())
            {
                throw new TException("length of data types does not equal to length of values!", null);
            }

            return new TSInsertStringRecordReq(sessionId, deviceId, measurements, values, timestamp)
            {
                IsAligned = isAligned
            };
        }
        public TSInsertStringRecordsReq GenInsertStringRecordsReq(List<string> deviceIds, List<List<string>> measurementsList,
            List<List<string>> valuesList, List<long> timestamps, long sessionId, bool isAligned = false)
        {
            if (valuesList.Count() != measurementsList.Count())
            {
                throw new TException("length of data types does not equal to length of values!", null);
            }

            return new TSInsertStringRecordsReq(sessionId, deviceIds, measurementsList, valuesList, timestamps)
            {
                IsAligned = isAligned
            };
        }

        public TSInsertRecordsReq GenInsertRecordsReq(List<string> deviceId, List<RowRecord> rowRecords,
            long sessionId)
        {
            var measurementLst = rowRecords.Select(x => x.Measurements).ToList();
            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();
            var valuesLstInBytes = rowRecords.Select(row => row.ToBytes()).ToList();

            return new TSInsertRecordsReq(sessionId, deviceId, measurementLst, valuesLstInBytes, timestampLst);
        }
        public async Task<int> InsertStringRecordAsync(string deviceId, List<string> measurements, List<string> values,
            long timestamp)
        {
            var client = _clients.Take();
            var req = GenInsertStrRecordReq(deviceId, measurements, values, timestamp, client.SessionId);
            try
            {
                var status = await client.ServiceClient.insertStringRecordAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one string record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertStringRecordAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting a string record", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedStringRecordAsync(string deviceId, List<string> measurements, List<string> values,
            long timestamp)
        {
            var client = _clients.Take();
            var req = GenInsertStrRecordReq(deviceId, measurements, values, timestamp, client.SessionId, true);
            try
            {
                var status = await client.ServiceClient.insertStringRecordAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertStringRecordAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting record", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertStringRecordsAsync(List<string> deviceIds, List<List<string>> measurements, List<List<string>> values,
            List<long> timestamps)
        {
            var client = _clients.Take();
            var req = GenInsertStringRecordsReq(deviceIds, measurements, values, timestamps, client.SessionId);
            try
            {
                var status = await client.ServiceClient.insertStringRecordsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert string records to devices {0}， server message: {1}", deviceIds, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);


            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertStringRecordsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert string records to devices {0}， server message: {1}", deviceIds, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);


                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting string records", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedStringRecordsAsync(List<string> deviceIds, List<List<string>> measurements, List<List<string>> values,
            List<long> timestamps)
        {
            var client = _clients.Take();
            var req = GenInsertStringRecordsReq(deviceIds, measurements, values, timestamps, client.SessionId, true);
            try
            {
                var status = await client.ServiceClient.insertStringRecordsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert string records to devices {0}， server message: {1}", deviceIds, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);


            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertStringRecordsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert string records to devices {0}， server message: {1}", deviceIds, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);


                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting string records", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertRecordsAsync(List<string> deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();

            var request = GenInsertRecordsReq(deviceId, rowRecords, client.SessionId);

            try
            {
                var status = await client.ServiceClient.insertRecordsAsync(request);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                request.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordsAsync(request);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting records", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedRecordsAsync(List<string> deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();

            var request = GenInsertRecordsReq(deviceId, rowRecords, client.SessionId);
            request.IsAligned = true;
            // ASSERT that the insert plan is aligned
            System.Diagnostics.Debug.Assert(request.IsAligned == true);

            try
            {
                var status = await client.ServiceClient.insertRecordsAsync(request);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                request.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordsAsync(request);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting records", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public TSInsertTabletReq GenInsertTabletReq(Tablet tablet, long sessionId)
        {
            return new TSInsertTabletReq(
                sessionId,
                tablet.DeviceId,
                tablet.Measurements,
                tablet.GetBinaryValues(),
                tablet.GetBinaryTimestamps(),
                tablet.GetDataTypes(),
                tablet.RowNumber);
        }

        public async Task<int> InsertTabletAsync(Tablet tablet)
        {
            var client = _clients.Take();
            var req = GenInsertTabletReq(tablet, client.SessionId);

            try
            {
                var status = await client.ServiceClient.insertTabletAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one tablet to device {0}, server message: {1}", tablet.DeviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one tablet to device {0}, server message: {1}", tablet.DeviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting tablet", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedTabletAsync(Tablet tablet)
        {
            var client = _clients.Take();
            var req = GenInsertTabletReq(tablet, client.SessionId);
            req.IsAligned = true;

            try
            {
                var status = await client.ServiceClient.insertTabletAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one aligned tablet to device {0}, server message: {1}", tablet.DeviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one aligned tablet to device {0}, server message: {1}", tablet.DeviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting tablet", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }


        public TSInsertTabletsReq GenInsertTabletsReq(List<Tablet> tabletLst, long sessionId)
        {
            var deviceIdLst = new List<string>();
            var measurementsLst = new List<List<string>>();
            var valuesLst = new List<byte[]>();
            var timestampsLst = new List<byte[]>();
            var typeLst = new List<List<int>>();
            var sizeLst = new List<int>();

            foreach (var tablet in tabletLst)
            {
                var dataTypeValues = tablet.GetDataTypes();
                deviceIdLst.Add(tablet.DeviceId);
                measurementsLst.Add(tablet.Measurements);
                valuesLst.Add(tablet.GetBinaryValues());
                timestampsLst.Add(tablet.GetBinaryTimestamps());
                typeLst.Add(dataTypeValues);
                sizeLst.Add(tablet.RowNumber);
            }

            return new TSInsertTabletsReq(
                sessionId,
                deviceIdLst,
                measurementsLst,
                valuesLst,
                timestampsLst,
                typeLst,
                sizeLst);
        }

        public async Task<int> InsertTabletsAsync(List<Tablet> tabletLst)
        {
            var client = _clients.Take();
            var req = GenInsertTabletsReq(tabletLst, client.SessionId);

            try
            {
                var status = await client.ServiceClient.insertTabletsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple tablets, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertTabletsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple tablets, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting tablets", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertAlignedTabletsAsync(List<Tablet> tabletLst)
        {
            var client = _clients.Take();
            var req = GenInsertTabletsReq(tabletLst, client.SessionId);
            req.IsAligned = true;

            try
            {
                var status = await client.ServiceClient.insertTabletsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple aligned tablets, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertTabletsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple aligned tablets, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting tablets", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertRecordsOfOneDeviceAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var sortedRowRecords = rowRecords.OrderBy(x => x.Timestamps).ToList();
            return await InsertRecordsOfOneDeviceSortedAsync(deviceId, sortedRowRecords);
        }
        public async Task<int> InsertAlignedRecordsOfOneDeviceAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var sortedRowRecords = rowRecords.OrderBy(x => x.Timestamps).ToList();
            return await InsertAlignedRecordsOfOneDeviceSortedAsync(deviceId, sortedRowRecords);
        }
        public async Task<int> InsertStringRecordsOfOneDeviceAsync(string deviceId, List<long> timestamps,
            List<List<string>> measurementsList, List<List<string>> valuesList)
        {
            var joined = timestamps.Zip(measurementsList, (t, m) => new { t, m })
                .Zip(valuesList, (tm, v) => new { tm.t, tm.m, v })
                .OrderBy(x => x.t);

            var sortedTimestamps = joined.Select(x => x.t).ToList();
            var sortedMeasurementsList = joined.Select(x => x.m).ToList();
            var sortedValuesList = joined.Select(x => x.v).ToList();

            return await InsertStringRecordsOfOneDeviceSortedAsync(deviceId, sortedTimestamps, sortedMeasurementsList, sortedValuesList, false);
        }
        public async Task<int> InsertAlignedStringRecordsOfOneDeviceAsync(string deviceId, List<long> timestamps,
            List<List<string>> measurementsList, List<List<string>> valuesList)
        {
            var joined = timestamps.Zip(measurementsList, (t, m) => new { t, m })
                .Zip(valuesList, (tm, v) => new { tm.t, tm.m, v })
                .OrderBy(x => x.t);

            var sortedTimestamps = joined.Select(x => x.t).ToList();
            var sortedMeasurementsList = joined.Select(x => x.m).ToList();
            var sortedValuesList = joined.Select(x => x.v).ToList();

            return await InsertStringRecordsOfOneDeviceSortedAsync(deviceId, sortedTimestamps, sortedMeasurementsList, sortedValuesList, true);
        }
        public async Task<int> InsertStringRecordsOfOneDeviceSortedAsync(string deviceId, List<long> timestamps,
            List<List<string>> measurementsList, List<List<string>> valuesList, bool isAligned)
        {
            var client = _clients.Take();

            if (!_utilFunctions.IsSorted(timestamps))
            {
                throw new TException("insert string records of one device error: timestamp not sorted", null);
            }

            var req = GenInsertStringRecordsOfOneDeviceReq(deviceId, timestamps, measurementsList, valuesList, client.SessionId, isAligned);
            try
            {
                var status = await client.ServiceClient.insertStringRecordsOfOneDeviceAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert string records of one device, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertStringRecordsOfOneDeviceAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert string records of one device, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting string records of one device", ex);
                }
            }
            finally
            {
                _clients.Add(client);

            }
        }
        private TSInsertStringRecordsOfOneDeviceReq GenInsertStringRecordsOfOneDeviceReq(string deviceId,
            List<long> timestamps, List<List<string>> measurementsList, List<List<string>> valuesList,
             long sessionId, bool isAligned = false)
        {
            return new TSInsertStringRecordsOfOneDeviceReq(
                sessionId,
                deviceId,
                measurementsList,
                valuesList,
                timestamps)
            {
                IsAligned = isAligned
            };
        }
        private TSInsertRecordsOfOneDeviceReq GenInsertRecordsOfOneDeviceRequest(
            string deviceId,
            List<RowRecord> records,
            long sessionId)
        {
            var values = records.Select(row => row.ToBytes());
            var measurementsLst = records.Select(x => x.Measurements).ToList();
            var timestampLst = records.Select(x => x.Timestamps).ToList();

            return new TSInsertRecordsOfOneDeviceReq(
                sessionId,
                deviceId,
                measurementsLst,
                values.ToList(),
                timestampLst);
        }

        public async Task<int> InsertRecordsOfOneDeviceSortedAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();

            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();

            if (!_utilFunctions.IsSorted(timestampLst))
            {
                throw new TException("insert records of one device error: timestamp not sorted", null);
            }

            var req = GenInsertRecordsOfOneDeviceRequest(deviceId, rowRecords, client.SessionId);

            try
            {
                var status = await client.ServiceClient.insertRecordsOfOneDeviceAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert records of one device, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordsOfOneDeviceAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert records of one device, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting records of one device", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<int> InsertAlignedRecordsOfOneDeviceSortedAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();

            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();

            if (!_utilFunctions.IsSorted(timestampLst))
            {
                throw new TException("insert records of one device error: timestamp not sorted", null);
            }

            var req = GenInsertRecordsOfOneDeviceRequest(deviceId, rowRecords, client.SessionId);
            req.IsAligned = true;

            try
            {
                var status = await client.ServiceClient.insertRecordsOfOneDeviceAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert aligned records of one device, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.insertRecordsOfOneDeviceAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert aligned records of one device, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when inserting aligned records of one device", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertRecordAsync(string deviceId, RowRecord record)
        {
            var client = _clients.Take();

            var req = new TSInsertRecordReq(
                client.SessionId,
                deviceId,
                record.Measurements,
                record.ToBytes(),
                record.Timestamps);

            try
            {
                var status = await client.ServiceClient.testInsertRecordAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.testInsertRecordAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when test inserting one record", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertRecordsAsync(List<string> deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();
            var req = GenInsertRecordsReq(deviceId, rowRecords, client.SessionId);

            try
            {
                var status = await client.ServiceClient.testInsertRecordsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.testInsertRecordsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when test inserting multiple records", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertTabletAsync(Tablet tablet)
        {
            var client = _clients.Take();

            var req = GenInsertTabletReq(tablet, client.SessionId);

            try
            {
                var status = await client.ServiceClient.testInsertTabletAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert one tablet to device {0}, server message: {1}", tablet.DeviceId,
                        status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.testInsertTabletAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert one tablet to device {0}, server message: {1}", tablet.DeviceId,
                            status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when test inserting one tablet", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertTabletsAsync(List<Tablet> tabletLst)
        {
            var client = _clients.Take();

            var req = GenInsertTabletsReq(tabletLst, client.SessionId);

            try
            {
                var status = await client.ServiceClient.testInsertTabletsAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("insert multiple tablets, message: {0}", status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.testInsertTabletsAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("insert multiple tablets, message: {0}", status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when test inserting multiple tablets", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<SessionDataSet> ExecuteQueryStatementAsync(string sql)
        {
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = _clients.Take();
            var req = new TSExecuteStatementReq(client.SessionId, sql, client.StatementId)
            {
                FetchSize = _fetchSize
            };
            try
            {
                resp = await client.ServiceClient.executeQueryStatementAsync(req);
                status = resp.Status;
            }
            catch (TException e)
            {
                _clients.Add(client);

                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                req.StatementId = client.StatementId;
                try
                {
                    resp = await client.ServiceClient.executeQueryStatementAsync(req);
                    status = resp.Status;
                }
                catch (TException ex)
                {
                    _clients.Add(client);
                    throw new TException("Error occurs when executing query statement", ex);
                }
            }

            if (_utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode) == -1)
            {
                _clients.Add(client);

                throw new TException("execute query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet(sql, resp, _clients, client.StatementId)
            {
                FetchSize = _fetchSize,
            };

            return sessionDataset;
        }
        public async Task<SessionDataSet> ExecuteStatementAsync(string sql)
        {
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = _clients.Take();
            var req = new TSExecuteStatementReq(client.SessionId, sql, client.StatementId)
            {
                FetchSize = _fetchSize
            };
            try
            {
                resp = await client.ServiceClient.executeStatementAsync(req);
                status = resp.Status;
            }
            catch (TException e)
            {
                _clients.Add(client);

                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                req.StatementId = client.StatementId;
                try
                {
                    resp = await client.ServiceClient.executeStatementAsync(req);
                    status = resp.Status;
                }
                catch (TException ex)
                {
                    _clients.Add(client);
                    throw new TException("Error occurs when executing query statement", ex);
                }
            }

            if (_utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode) == -1)
            {
                _clients.Add(client);

                throw new TException("execute query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet(sql, resp, _clients, client.StatementId)
            {
                FetchSize = _fetchSize,
            };

            return sessionDataset;
        }

        public async Task<int> ExecuteNonQueryStatementAsync(string sql)
        {
            var client = _clients.Take();
            var req = new TSExecuteStatementReq(client.SessionId, sql, client.StatementId);

            try
            {
                var resp = await client.ServiceClient.executeUpdateStatementAsync(req);
                var status = resp.Status;

                if (_debugMode)
                {
                    _logger.LogInformation("execute non-query statement {0} message: {1}", sql, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                req.StatementId = client.StatementId;
                try
                {
                    var resp = await client.ServiceClient.executeUpdateStatementAsync(req);
                    var status = resp.Status;

                    if (_debugMode)
                    {
                        _logger.LogInformation("execute non-query statement {0} message: {1}", sql, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when executing non-query statement", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<SessionDataSet> ExecuteRawDataQuery(List<string> paths, long startTime, long endTime)
        {
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = _clients.Take();
            var req = new TSRawDataQueryReq(client.SessionId, paths, startTime, endTime, client.StatementId)
            {
                FetchSize = _fetchSize,
                EnableRedirectQuery = false
            };
            try
            {
                resp = await client.ServiceClient.executeRawDataQueryAsync(req);
                status = resp.Status;
            }
            catch (TException e)
            {
                _clients.Add(client);

                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                req.StatementId = client.StatementId;
                try
                {
                    resp = await client.ServiceClient.executeRawDataQueryAsync(req);
                    status = resp.Status;
                }
                catch (TException ex)
                {
                    _clients.Add(client);
                    throw new TException("Error occurs when executing raw data query", ex);
                }
            }

            if (_utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode) == -1)
            {
                _clients.Add(client);

                throw new TException("execute raw data query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet("", resp, _clients, client.StatementId)
            {
                FetchSize = _fetchSize,
            };

            return sessionDataset;
        }
        public async Task<SessionDataSet> ExecuteLastDataQueryAsync(List<string> paths, long lastTime)
        {
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = _clients.Take();
            var req = new TSLastDataQueryReq(client.SessionId, paths, lastTime, client.StatementId)
            {
                FetchSize = _fetchSize,
                EnableRedirectQuery = false
            };
            try
            {
                resp = await client.ServiceClient.executeLastDataQueryAsync(req);
                status = resp.Status;
            }
            catch (TException e)
            {
                _clients.Add(client);

                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                req.StatementId = client.StatementId;
                try
                {
                    resp = await client.ServiceClient.executeLastDataQueryAsync(req);
                    status = resp.Status;
                }
                catch (TException ex)
                {
                    _clients.Add(client);
                    throw new TException("Error occurs when executing last data query", ex);
                }
            }

            if (_utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode) == -1)
            {
                _clients.Add(client);

                throw new TException("execute last data query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet("", resp, _clients, client.StatementId)
            {
                FetchSize = _fetchSize,
            };

            return sessionDataset;
        }

        public async Task<int> CreateSchemaTemplateAsync(Template template)
        {
            var client = _clients.Take();
            var req = new TSCreateSchemaTemplateReq(client.SessionId, template.Name, template.ToBytes());
            try
            {
                var status = await client.ServiceClient.createSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("create schema template {0} message: {1}", template.Name, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.createSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("create schema template {0} message: {1}", template.Name, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when creating schema template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DropSchemaTemplateAsync(string templateName)
        {
            var client = _clients.Take();
            var req = new TSDropSchemaTemplateReq(client.SessionId, templateName);
            try
            {
                var status = await client.ServiceClient.dropSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("drop schema template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.dropSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("drop schema template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when dropping schema template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> SetSchemaTemplateAsync(string templateName, string prefixPath)
        {
            var client = _clients.Take();
            var req = new TSSetSchemaTemplateReq(client.SessionId, templateName, prefixPath);
            try
            {
                var status = await client.ServiceClient.setSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("set schema template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.setSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("set schema template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when setting schema template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> UnsetSchemaTemplateAsync(string prefixPath, string templateName)
        {
            var client = _clients.Take();
            var req = new TSUnsetSchemaTemplateReq(client.SessionId, prefixPath, templateName);
            try
            {
                var status = await client.ServiceClient.unsetSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("unset schema template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.unsetSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("unset schema template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when unsetting schema template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteNodeInTemplateAsync(string templateName, string path)
        {
            var client = _clients.Take();
            var req = new TSPruneSchemaTemplateReq(client.SessionId, templateName, path);
            try
            {
                var status = await client.ServiceClient.pruneSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("delete node in template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.pruneSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("delete node in template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);

                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when deleting node in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> CountMeasurementsInTemplateAsync(string name)
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, name, (int)TemplateQueryType.COUNT_MEASUREMENTS);
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("count measurements in template {0} message: {1}", name, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Count;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("count measurements in template {0} message: {1}", name, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Count;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when counting measurements in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<bool> IsMeasurementInTemplateAsync(string templateName, string path)
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, templateName, (int)TemplateQueryType.IS_MEASUREMENT);
            req.Measurement = path;
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("is measurement in template {0} message: {1}", templateName, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Result;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("is measurement in template {0} message: {1}", templateName, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Result;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when checking measurement in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<bool> IsPathExistInTemplate(string templateName, string path)
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, templateName, (int)TemplateQueryType.PATH_EXIST);
            req.Measurement = path;
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("is path exist in template {0} message: {1}", templateName, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Result;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("is path exist in template {0} message: {1}", templateName, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Result;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when checking path exist in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<List<string>> ShowMeasurementsInTemplateAsync(string templateName, string pattern = "")
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, templateName, (int)TemplateQueryType.SHOW_MEASUREMENTS);
            req.Measurement = pattern;
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("get measurements in template {0} message: {1}", templateName, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Measurements;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("get measurements in template {0} message: {1}", templateName, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Measurements;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when getting measurements in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<List<string>> ShowAllTemplatesAsync()
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, "", (int)TemplateQueryType.SHOW_TEMPLATES);
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("get all templates message: {0}", status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Measurements;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("get all templates message: {0}", status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Measurements;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when getting all templates", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<List<string>> ShowPathsTemplateSetOnAsync(string templateName)
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, templateName, (int)TemplateQueryType.SHOW_SET_TEMPLATES);
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("get paths template set on {0} message: {1}", templateName, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Measurements;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("get paths template set on {0} message: {1}", templateName, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Measurements;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when getting paths template set on", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }
        public async Task<List<string>> ShowPathsTemplateUsingOnAsync(string templateName)
        {
            var client = _clients.Take();
            var req = new TSQueryTemplateReq(client.SessionId, templateName, (int)TemplateQueryType.SHOW_USING_TEMPLATES);
            try
            {
                var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                var status = resp.Status;
                if (_debugMode)
                {
                    _logger.LogInformation("get paths template using on {0} message: {1}", templateName, status.Message);
                }

                _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                return resp.Measurements;
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var resp = await client.ServiceClient.querySchemaTemplateAsync(req);
                    var status = resp.Status;
                    if (_debugMode)
                    {
                        _logger.LogInformation("get paths template using on {0} message: {1}", templateName, status.Message);
                    }

                    _utilFunctions.VerifySuccess(status, SuccessCode, RedirectRecommendCode);
                    return resp.Measurements;
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when getting paths template using on", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
#if NET461_OR_GREATER || NETSTANDARD2_0
#else
                    _clients.ClientQueue.Clear();
#endif
                }
                _clients = null;
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}