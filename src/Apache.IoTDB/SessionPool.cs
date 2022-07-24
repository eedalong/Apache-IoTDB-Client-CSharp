using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
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
    public class SessionPool
    {
        private static int SuccessCode => 200;
        private static readonly TSProtocolVersion ProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

        private readonly string _username;
        private readonly string _password;
        private bool _enableRpcCompression;
        private string _zoneId;
        private readonly string _host;
        private readonly int _port;
        private readonly int _fetchSize;
        private readonly int _poolSize = 4;
        private readonly Utils _utilFunctions = new Utils();


        private bool _debugMode;
        private bool _isClose = true;
        private ConcurrentClientQueue _clients;
        private ILogger _logger;

        public SessionPool(string host, int port, int poolSize)
        {
            // init success code 
            _host = host;
            _port = port;
            _username = "root";
            _password = "root";
            _zoneId = "UTC+08:00";
            _fetchSize = 1024;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host,
            int port,
            string username,
            string password,
            int poolSize = 8)
        {
            _host = host;
            _port = port;
            _password = password;
            _username = username;
            _zoneId = "UTC+08:00";
            _fetchSize = 1024;
            _debugMode = false;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host,
            int port,
            string username,
            string password,
            int fetchSize,
            int poolSize = 8)
        {
            _host = host;
            _port = port;
            _username = username;
            _password = password;
            _fetchSize = fetchSize;
            _zoneId = "UTC+08:00";
            _debugMode = false;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host,
            int port,
            string username = "root",
            string password = "root",
            int fetchSize = 1000,
            string zoneId = "UTC+08:00",
            int poolSize = 8,
            bool enableRpcCompression = true
            )
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
        }

        ILoggerFactory factory;
        public void OpenDebugMode(Action<ILoggingBuilder> configure)
        {
            _debugMode = true;
            factory= LoggerFactory.Create(configure);
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


            for (var index = 0; index < _poolSize; index++)
            {
                _clients.Add(await CreateAndOpen(_enableRpcCompression, cancellationToken));
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

        private async Task<Client> CreateAndOpen(bool enableRpcCompression, CancellationToken cancellationToken = default)
        {
            var tcpClient = new TcpClient(_host, _port);

            var transport = new TFramedTransport(new TSocketTransport(tcpClient, null));

            if (!transport.IsOpen)
            {
                await transport.OpenAsync(cancellationToken);
            }

            var client = enableRpcCompression ?
                new TSIService.Client(new TCompactProtocol(transport)) :
                new TSIService.Client(new TBinaryProtocol(transport));

            var openReq = new TSOpenSessionReq(ProtocolVersion, _zoneId)
            {
                Username = _username,
                Password = _password
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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
                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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
                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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
                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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
                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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
            List<string> values, long timestamp, long sessionId)
        {
            if (values.Count() != measurements.Count())
            {
                throw new TException("length of data types does not equal to length of values!", null);
            }

            return new TSInsertStringRecordReq(sessionId, deviceId, measurements, values, timestamp);
        }

        public TSInsertRecordsReq GenInsertRecordsReq(List<string> deviceId, List<RowRecord> rowRecords,
            long sessionId)
        {
            //TODO
            var measurementLst = rowRecords.Select(x => x.Measurements).ToList();
            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();
            var valuesLstInBytes = rowRecords.Select(row => row.ToBytes()).ToList();

            return new TSInsertRecordsReq(sessionId, deviceId, measurementLst, valuesLstInBytes, timestampLst);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

            if (_utilFunctions.VerifySuccess(status, SuccessCode) == -1)
            {
                _clients.Add(client);

                throw new TException("execute query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet(sql, resp, _clients)
            {
                FetchSize = _fetchSize
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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
        public async Task<int> AddAlignedMeasurementsInTemplateAsync(string templateName, List<MeasurementNode> measurementNodes)
        {
            var client = _clients.Take();
            var measurements = measurementNodes.ConvertAll(m => m.Name);
            var dataTypes = measurementNodes.ConvertAll(m => (int)m.DataType);
            var encodings = measurementNodes.ConvertAll(m => (int)m.Encoding);
            var compressors = measurementNodes.ConvertAll(m => (int)m.Compressor);
            var req = new TSAppendSchemaTemplateReq(client.SessionId, templateName, true, measurements, dataTypes, encodings, compressors);
            try
            {
                var status = await client.ServiceClient.appendSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("add aligned measurements in template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode);
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.appendSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("add aligned measurements in template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when adding aligned measurements in template", ex);
                }
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> AddUnalignedMeasurementsInTemplateAsync(string templateName, List<MeasurementNode> measurementNodes)
        {
            var client = _clients.Take();
            var measurements = measurementNodes.ConvertAll(m => m.Name);
            var dataTypes = measurementNodes.ConvertAll(m => (int)m.DataType);
            var encodings = measurementNodes.ConvertAll(m => (int)m.Encoding);
            var compressors = measurementNodes.ConvertAll(m => (int)m.Compressor);
            var req = new TSAppendSchemaTemplateReq(client.SessionId, templateName, false, measurements, dataTypes, encodings, compressors);
            try
            {
                var status = await client.ServiceClient.appendSchemaTemplateAsync(req);

                if (_debugMode)
                {
                    _logger.LogInformation("add unaligned measurements in template {0} message: {1}", templateName, status.Message);
                }

                return _utilFunctions.VerifySuccess(status, SuccessCode);
            }
            catch (TException e)
            {
                await Open(_enableRpcCompression);
                client = _clients.Take();
                req.SessionId = client.SessionId;
                try
                {
                    var status = await client.ServiceClient.appendSchemaTemplateAsync(req);

                    if (_debugMode)
                    {
                        _logger.LogInformation("add unaligned measurements in template {0} message: {1}", templateName, status.Message);
                    }

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
                }
                catch (TException ex)
                {
                    throw new TException("Error occurs when adding unaligned measurements in template", ex);
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

                return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    return _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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

                _utilFunctions.VerifySuccess(status, SuccessCode);
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

                    _utilFunctions.VerifySuccess(status, SuccessCode);
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









    }
}