using Thrift;
using Thrift.Transport;
using Thrift.Protocol;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using Thrift.Transport.Client;
using iotdb_client_csharp.client.utils;
using NLog;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Collections.Concurrent;
namespace iotdb_client_csharp.client{
    public class SessionPool{
        private string username, password, zoneId, host;
        public int SUCCESS_CODE{
           get{return 200;}
        }
        private int port, fetch_size;
        private int pool_size = 4;
        private bool debug_mode = false;
        private bool is_close = true;
        private ConcurentClientQueue client_lst;
        private NLog.Logger _logger;
        public Utils util_functions = new Utils();
        private static TSProtocolVersion protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

    public SessionPool(string host, int port, int pool_size){
           // init success code 
           this.host = host;
           this.port = port;
           this.username = "root";
           this.password = "root";
           this.zoneId = "UTC+08:00";
           this.fetch_size = 1024;
           this.pool_size = pool_size;
       }  
       public SessionPool(string host, int port, string username, string password, int pool_size=8){
           this.host = host;
           this.port = port;
           this.password = password;
           this.username = username;
           this.zoneId = "UTC+08:00";
           this.fetch_size = 1024;
           this.debug_mode = false;
           this.pool_size = pool_size;
       }
       public SessionPool(string host, int port, string username, string password, int fetch_size, int pool_size=8){
           this.host = host;
           this.port = port;
           this.username = username;
           this.password = password;
           this.fetch_size = fetch_size;
           this.zoneId = "UTC+08:00";
           this.debug_mode = false;
           this.pool_size = pool_size;

       }
        public SessionPool(string host, int port, string username="root", string password="root", int fetch_size=1000, string zoneId = "UTC+08:00", int pool_size=8){
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.zoneId = zoneId;
            this.fetch_size = fetch_size;
            this.debug_mode = false;
            this.pool_size = pool_size;
        }
        public void open_debug_mode(NLog.Config.LoggingConfiguration config=null){
            this.debug_mode = true;
            if(config == null){
                config = new NLog.Config.LoggingConfiguration();
                var logconsole = new NLog.Targets.ConsoleTarget("logconsole");
                config.AddRule(LogLevel.Debug, LogLevel.Fatal, logconsole);
                NLog.LogManager.Configuration = config;
                _logger = NLog.LogManager.GetCurrentClassLogger();
            }else{
                NLog.LogManager.Configuration = config;
                _logger = NLog.LogManager.GetCurrentClassLogger();
            }
        }
        public void close_debug_mode(){
            this.debug_mode = false;
        }

        public void open(bool enableRPCCompression){
            client_lst = new ConcurentClientQueue();
            for(int index = 0; index < pool_size; index++){
                client_lst.Add(create_and_open(enableRPCCompression));
            }
        }
        public bool is_open(){
            return !is_close;
        }
        public void close(){
            if(is_close){
                return;
            }
            foreach(var client in client_lst.client_queue.AsEnumerable()){
                var req = new TSCloseSessionReq(client.sessionId);
                try{
                    var task = client.client.closeSessionAsync(req);
                    task.Wait();
                }
                catch(TException e){
                    var message = String.Format("Error occurs when closing session at server. Maybe server is down");
                    throw new TException(message, e);
                }
                finally{
                    is_close = true;
                    if (client.transport != null){
                        client.transport.Close();
                    }
                }
            }
        }
        public void set_time_zone(string zoneId){
            this.zoneId = zoneId;
            foreach(var client in client_lst.client_queue.AsEnumerable()){
                var req = new TSSetTimeZoneReq(client.sessionId, zoneId);
                try{
                    var task = client.client.setTimeZoneAsync(req);
                    task.Wait();
                    if(debug_mode){
                        _logger.Info("setting time zone_id as {0}, server message:{1}", zoneId, task.Result.Message);
                    }
                }
                catch(TException e ){
                    var message = String.Format("could not set time zone");
                    throw new TException(message, e); 
                }
            }
        }
        public string get_time_zone(){
            TSGetTimeZoneResp resp;
            if(zoneId != ""){
                return zoneId;
            }
            var client = client_lst.Take();
            try{
                var task = client.client.getTimeZoneAsync(client.sessionId);
                task.Wait();
                resp = task.Result;
            }
            catch(TException e){
                client_lst.Add(client);
                var message = String.Format("counld not get time zone");
                throw new TException(message, e); 
            }
            client_lst.Add(client);
            return resp.TimeZone;
        }

        public Client create_and_open(bool enableRPCCompression){          
            TcpClient tcp_client = new TcpClient(this.host, this.port);
            TSIService.Client client;
            long sessionId, statementId;
            var transport = new TFramedTransport(new TSocketTransport(tcp_client, null));
            // this will fail remote server access
            //this.transport = new TFramedTransport(new TSocketTransport(this.host, this.port, new TConfiguration()));
            if(!transport.IsOpen){
                try{
                    var task = transport.OpenAsync(new CancellationToken());
                    task.Wait();
                }
                catch(TTransportException){
                    throw;
                }
            }
            if(enableRPCCompression){
                client = new TSIService.Client(new TCompactProtocol(transport));
            }else{
                client = new TSIService.Client(new TBinaryProtocol(transport));
            }
            var open_req = new TSOpenSessionReq(protocol_version, zoneId);
            open_req.Username = username;
            open_req.Password = password;
            try{
                var task = client.openSessionAsync(open_req);
                task.Wait();
                var open_resp = task.Result;
                if(open_resp.ServerProtocolVersion != protocol_version){
                    var message = String.Format("Protocol Differ, Client version is {0} but Server version is {1}", protocol_version, open_resp.ServerProtocolVersion);
                    throw new TException(message, null);
                }
                if (open_resp.ServerProtocolVersion == 0){
                    throw new TException("Protocol not supported", null);
                }
                sessionId = open_resp.SessionId;
                var statement_task = client.requestStatementIdAsync(sessionId);
                statement_task.Wait();
                statementId = statement_task.Result;
            }
            catch(Exception){
                transport.Close();
                throw;
            }
            is_close = false; 
            var return_client = new Client();
            return_client.client = client;
            return_client.sessionId = sessionId;
            return_client.statementId = statementId;
            return_client.transport = transport;
            return return_client;       
        }
        public async Task<int> set_storage_group_async(string group_name){
            TSStatus status;
            var client = client_lst.Take();
            try{
                status = await client.client.setStorageGroupAsync(client.sessionId, group_name);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("set storage group {0} failed", group_name);
                throw new TException(err_msg, e);
            }
           
            if(debug_mode){
                _logger.Info("set storage group {0} successfully, server message is {1}", group_name, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }

        public async Task<int> create_time_series_async(string ts_path, TSDataType data_type, TSEncoding encoding, Compressor compressor){
            TSStatus status;
            var client = client_lst.Take();
            var req = new TSCreateTimeseriesReq(client.sessionId, ts_path, (int)data_type, (int)encoding, (int)compressor);
            try{
                status = await client.client.createTimeseriesAsync(req);
            }
            catch(TException e){
                client_lst.Add(client); 
                var err_msg = String.Format("create time series {0} failed", ts_path);   
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("creating time series {0} successfully, server message is {1}", ts_path, status.Message);
            }
            return util_functions.verify_success(status, SUCCESS_CODE); 
        }
        public async Task<int> delete_storage_group_async(string group_name){
            TSStatus status;
            var client = client_lst.Take();
            try{
                status = await client.client.deleteStorageGroupsAsync(client.sessionId, new List<string>{group_name});
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("delete storage group {0} failed", group_name);
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                var message = String.Format("delete storage group {0} successfully, server message is {1}", group_name, status.Message);
                _logger.Info(message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public async Task<int> delete_storage_groups_async(List<string> group_names){
            var client = client_lst.Take();

            TSStatus status;
            try{
                status = await client.client.deleteStorageGroupsAsync(client.sessionId, group_names);
                
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("delete storage group(s) {0} failed", group_names);
                throw new TException(err_msg, e);           
            }
            if(debug_mode){
                _logger.Info("delete storage group(s) {0} successfully, server message is {1}", group_names, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public async Task<int> create_multi_time_series_async(List<string> ts_path_lst, List<TSDataType> data_type_lst, List<TSEncoding> encoding_lst, List<Compressor> compressor_lst){
            var client = client_lst.Take();
            var data_types = data_type_lst.ConvertAll<int>(x => (int)x);
            var encodings = encoding_lst.ConvertAll<int>(x => (int)x);
            var compressors = compressor_lst.ConvertAll<int>(x => (int)x);
            TSStatus status;
            var req = new TSCreateMultiTimeseriesReq(client.sessionId, ts_path_lst, data_types, encodings, compressors);
            try{
                status = await client.client.createMultiTimeseriesAsync(req);
                
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("create multiple time series {0} failed", ts_path_lst);
                throw new TException(err_msg, e);             
            }
            if(debug_mode){
                _logger.Info("creating multiple time series {0}, server message is {1}", ts_path_lst, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public async Task<int> delete_time_series_async(List<string> path_list){
            TSStatus status;
            var client = client_lst.Take();
            try{
                status = await client.client.deleteTimeseriesAsync(client.sessionId, path_list);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("delete time series {0} failed", path_list);
                throw new TException(err_msg, e);             
            }
            if(debug_mode){
                _logger.Info("deleting multiple time series {0}, server message is {1}", path_list, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public async Task<int> delete_time_series_async(string ts_path){
            return await delete_time_series_async(new List<string>{ts_path});
        }
        public async Task<bool> check_time_series_exists_async(string ts_path){
            // TBD by dalong
            try{
                string sql = "SHOW TIMESERIES " + ts_path;
                var session_dataset = await execute_query_statement_async(sql);
                return session_dataset.has_next();
            }
            catch(TException e){
                var err_msg = String.Format("could not check if certain time series exists");
                throw new TException(err_msg, e);
            }
        }
        public async Task<int> delete_data_async(List<string> ts_path_lst, long start_time, long end_time){
            var client = client_lst.Take();
            var req = new TSDeleteDataReq(client.sessionId, ts_path_lst, start_time, end_time);
            TSStatus status;
            try{
                status = await client.client.deleteDataAsync(req);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("data deletion fails because");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("delete data from {0}, server message is {1}", ts_path_lst, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }

        public TSInsertRecordReq gen_insert_record_req(string device_id, List<string> measurements, List<string> values, List<TSDataType> data_types, long timestamp, long session_id){
            if(values.Count() != data_types.Count() || values.Count() != measurements.Count()){
                var err_msg = "length of data types does not equal to length of values!";
                throw new TException(err_msg, null);
            }
            var values_in_bytes = util_functions.value_to_bytes(data_types, values);
            return new TSInsertRecordReq(session_id, device_id, measurements, values_in_bytes, timestamp);
        }
        public async Task<int> insert_record_async(string device_id, List<string> measurements, List<string> values, List<TSDataType> data_types, long timestamp){
            // TBD by Luzhan
            var client = client_lst.Take();
            var req = gen_insert_record_req(device_id, measurements, values, data_types, timestamp, client.sessionId);
            TSStatus status;
            try{
               status = await client.client.insertRecordAsync(req);
            }
            catch(TException e){
                client_lst.Add(client);
                Console.WriteLine(e);
                var err_msg = String.Format("Record insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert one record to device {0}， server message: {1}", device_id, status.Message);
            }
            client_lst.Add(client);

            return util_functions.verify_success(status, SUCCESS_CODE);
        }
         public TSInsertStringRecordReq gen_insert_str_record_req(string device_id, List<string> measurements, List<string> values, long timestamp, long session_id){
            if(values.Count() != measurements.Count()){
                var err_msg = "length of data types does not equal to length of values!";
                throw new TException(err_msg, null);
            }
            return new TSInsertStringRecordReq(session_id, device_id, measurements, values, timestamp);
        }
        public async Task<int> insert_record_async(string device_id, List<string> measurements, List<string> values, long timestamp){

            var client = client_lst.Take();
            var req = gen_insert_str_record_req(device_id, measurements, values, timestamp, client.sessionId);
            TSStatus status;
            try{
                status = await client.client.insertStringRecordAsync(req);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("record insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert one record to device {0} successfully, server message is {1}", device_id, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public TSInsertRecordsReq gen_insert_records_req(List<string> device_id, List<List<string>> measurements_lst, List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, List<long> timestamp_lst, long session_id){
            //TODO
            if(device_id.Count() != measurements_lst.Count() || timestamp_lst.Count() != data_types_lst.Count() || 
               device_id.Count() != timestamp_lst.Count()    || timestamp_lst.Count() != values_lst.Count()){
                   var err_msg = String.Format("deviceIds, times, measurementsList and valueList's size should be equal");
                   throw new TException(err_msg,null);
            }
            
            List<byte[]> values_lst_in_bytes = new List<byte[]>();
            for(int i = 0;i < values_lst.Count(); i++){
                var values = values_lst[i];
                var data_types = data_types_lst[i];
                var measurements = measurements_lst[i];
                if(values.Count() != data_types.Count() || values.Count() != measurements.Count()){
                    var err_msg = String.Format("deviceIds, times, measurementsList and valueList's size should be equal");
                    throw new TException(err_msg, null);
                }
                var values_in_bytes = util_functions.value_to_bytes(data_types, values);
                values_lst_in_bytes.Add(values_in_bytes);
            }

            return new TSInsertRecordsReq(session_id, device_id, measurements_lst, values_lst_in_bytes, timestamp_lst);
        }
        public async Task<int> insert_records_async(List<string> device_id, List<List<string>> measurements_lst, List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, List<long> timestamp_lst){
           
            var client = client_lst.Take();
            var req = gen_insert_records_req(device_id, measurements_lst, values_lst, data_types_lst, timestamp_lst, client.sessionId);
            TSStatus status;
            try{
                status = await client.client.insertRecordsAsync(req);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("Multiple records insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert multiple records to devices {0}, server message: {1}", device_id, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public TSInsertTabletReq gen_insert_tablet_req(Tablet tablet, long session_id){
            List<int> data_type_values = new List<int>(){};
            for(int i = 0; i < tablet.data_type_lst.Count(); i++){
                var data_type_value = (int)tablet.data_type_lst[i];
                data_type_values.Add(data_type_value);
            }
            return new TSInsertTabletReq(session_id, tablet.device_id, tablet.measurement_lst, tablet.get_binary_values(), tablet.get_binary_timestamps(), data_type_values, tablet.row_number);
        }
        public async Task<int> insert_tablet_async(Tablet tablet){
            var client = client_lst.Take();
            var req = gen_insert_tablet_req(tablet, client.sessionId);
            TSStatus status;
            try{
                status = await client.client.insertTabletAsync(req);
                
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("Tablet insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert one tablet to device {0}, server message: {1}", tablet.device_id, status.Message);
            }       
            client_lst.Add(client);     
            
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public TSInsertTabletsReq gen_insert_tablets_req(List<Tablet> tablet_lst, long session_id){
            List<string> device_id_lst = new List<string>(){};
            List<List<string>> measurements_lst = new List<List<string>>(){};
            List<byte[]> values_lst = new List<byte[]>(){};
            List<byte[]> timestamps_lst = new List<byte[]>(){};
            List<List<int>> type_lst = new List<List<int>>(){};
            List<int> size_lst = new List<int>(){};
            for(int i = 0; i < tablet_lst.Count(); i++){
                List<int> data_type_values = new List<int>(){};
                for(int j = 0;j < tablet_lst[i].data_type_lst.Count(); j++){
                    var data_type_value = (int)tablet_lst[i].data_type_lst[j];
                    data_type_values.Add(data_type_value);
                }
                device_id_lst.Add(tablet_lst[i].device_id);
                measurements_lst.Add(tablet_lst[i].measurement_lst);
                values_lst.Add(tablet_lst[i].get_binary_values());
                timestamps_lst.Add(tablet_lst[i].get_binary_timestamps());
                type_lst.Add(data_type_values);
                size_lst.Add(tablet_lst[i].row_number);
            }
            return new TSInsertTabletsReq(session_id, device_id_lst, measurements_lst, values_lst, timestamps_lst, type_lst, size_lst);
        }
        public async Task<int> insert_tablets_async(List<Tablet> tablet_lst){
            var client = client_lst.Take();
            var req = gen_insert_tablets_req(tablet_lst, client.sessionId);
            TSStatus status;
            try{
                status = await client.client.insertTabletsAsync(req);
               
            }
            catch(TException e){
                var err_msg = String.Format("Multiple tablets insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert multiple tablets, message: {0}", status.Message);
            }
            return util_functions.verify_success(status, SUCCESS_CODE);
        }
        public async Task<int> insert_records_of_one_device_async(string device_id, List<long> timestamp_lst, List<List<string>> measurements_lst, List<List<TSDataType>> data_types_lst, List<List<string>> values_lst){
            var sorted = timestamp_lst.Select((x, index) => (timestamp: x, measurements:measurements_lst[index], data_types:data_types_lst[index], values:values_lst[index])).OrderBy(x => x.timestamp).ToList();
            List<long> sorted_timestamp_lst = sorted.Select(x => x.timestamp).ToList();
            List<List<string>> sorted_measurements_lst = sorted.Select(x => x.measurements).ToList();
            List<List<TSDataType>> sorted_datatye_lst = sorted.Select(x => x.data_types).ToList();
            List<List<string>> sorted_value_lst = sorted.Select(x => x.values).ToList();
            return await insert_records_of_one_device_sorted_async(device_id, sorted_timestamp_lst, sorted_measurements_lst, sorted_datatye_lst, sorted_value_lst);

        }

        public TSInsertRecordsOfOneDeviceReq gen_insert_records_of_one_device_request(string device_id, List<long> timestamp_lst, List<List<string>> measurements_lst,  List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, long session_id){
            List<byte[]> binary_value_lst = new List<byte[]>(){};
            for(int i = 0; i < values_lst.Count(); i++){
                List<TSDataType> data_type_values = data_types_lst[i];
                for(int j = 0;j < data_types_lst[i].Count(); j++){
                    var data_type_value = (int)data_types_lst[i][j];
                }
                if(values_lst[i].Count() != data_type_values.Count() || values_lst[i].Count() != measurements_lst[i].Count()){
                    var err_msg = "insert records of one device error: deviceIds, times, measurementsList and valuesList's size should be equal";
                    throw new TException(err_msg, null);
                }
                var value_in_bytes = util_functions.value_to_bytes(data_type_values, values_lst[i]);
                binary_value_lst.Add(value_in_bytes);
            }
            return new TSInsertRecordsOfOneDeviceReq(session_id, device_id, measurements_lst, binary_value_lst, timestamp_lst);
        }
        public async Task<int> insert_records_of_one_device_sorted_async(string device_id, List<long> timestamp_lst, List<List<string>> measurements_lst, List<List<TSDataType>> data_types_lst, List<List<string>> values_lst){
            var client = client_lst.Take();
            var size = timestamp_lst.Count();
            if(size != measurements_lst.Count() || size != data_types_lst.Count() || size != values_lst.Count()){
                var err_msg = "insert records of one device error: types, times, measurementsList and valuesList's size should be equal";
                throw new TException(err_msg, null);
            }
            if(!util_functions.check_sorted(timestamp_lst)){
                var err_msg = "insert records of one device error: timestamp not sorted";
                throw new TException(err_msg, null);
            }
            var req = gen_insert_records_of_one_device_request(device_id, timestamp_lst, measurements_lst, values_lst, data_types_lst, client.sessionId);
            TSStatus status;
            try{
                status = await client.client.insertRecordsOfOneDeviceAsync(req);
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("Sorted records of one device insertion failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("insert records of one device, message: {0}", status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }


        public async Task<SessionDataSet>  execute_query_statement_async(string sql){
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = client_lst.Take();
            var req = new TSExecuteStatementReq(client.sessionId, sql, client.statementId);
            req.FetchSize = this.fetch_size;
            try{
                resp= await client.client.executeQueryStatementAsync(req);
                status = resp.Status;
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("could not execute query statement");
                throw new TException(err_msg, e);
            }
            if(util_functions.verify_success(status, SUCCESS_CODE) == -1){
                client_lst.Add(client);
                throw new TException("execute query failed", null);
            }
            client_lst.Add(client);
            var session_dataset = new SessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, resp.QueryId, client_lst, resp.QueryDataSet);
            session_dataset.fetch_size = fetch_size;
            return session_dataset;
        }
        public async Task<int> execute_non_query_statement_async(string sql){
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = client_lst.Take();
            var req = new TSExecuteStatementReq(client.sessionId, sql, client.statementId);
            try{
                resp = await client.client.executeUpdateStatementAsync(req);
                status = resp.Status;
            }
            catch(TException e){
                client_lst.Add(client);
                var err_msg = String.Format("execution of non-query statement failed");
                throw new TException(err_msg, e);
            }
            if(debug_mode){
                _logger.Info("execute non-query statement {0} message: {1}", sql, status.Message);
            }
            client_lst.Add(client);
            return util_functions.verify_success(status, SUCCESS_CODE);
        }

        
        
    }

 
}
