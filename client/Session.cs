using System.Net.Mime;
using Thrift;
using Thrift.Transport;
using Thrift.Protocol;
using Thrift.Server;
using System;
using System.Linq;
using System.Net.Sockets;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift.Collections;


using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;
using iotdb_client_csharp.client.utils;

namespace iotdb_client_csharp.client
{
    //public enum TSDataType{BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, NONE};
    //public enum TSEncoding{PLAIN, PLAIN_DICTIONARY, RLE, DIFF, TS_2DIFF, BITMAP, GORILLA_V1, REGULAR, GORILLA, NONE};
    //public enum Compressor{UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA, LZ4};

    public class Session
    {
       private string username, password, zoneId, host;
       public int SUCCESS_CODE{
           get{return 200;}
       }
       private int port, fetch_size;
       private long sessionId, statementId;
       private bool is_close = true;

       private TSIService.Client client; 
       private TFramedTransport transport;
       private static TSProtocolVersion protocol_version = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;


       public Session(string host, int port){
           // init success code 
           this.host = host;
           this.port = port;
           this.username = "root";
           this.password = "root";
           this.zoneId = "UTC+08:00";
           this.fetch_size = 1024;
       } 
       public Session(string host, int port, string username, string password){
           this.host = host;
           this.port = port;
           this.password = password;
           this.username = username;
           this.zoneId = "UTC+08:00";
           this.fetch_size = 1024;
       }
       public Session(string host, int port, string username, string password, int fetch_size){
           this.host = host;
           this.port = port;
           this.username = username;
           this.password = password;
           this.fetch_size = fetch_size;
           this.zoneId = "UTC+08:00";

       }
        public Session(string host, int port, string username="root", string password="root", int fetch_size=10000, string zoneId = "UTC+08:00"){
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.zoneId = zoneId;
            this.fetch_size = fetch_size;
        }
        public void open(bool enableRPCCompression){
            if(!is_close){
                return ;
            }
            this.transport = new TFramedTransport(new TSocketTransport(this.host, this.port, new TConfiguration()));
            if(!transport.IsOpen){
                try{
                    var task = transport.OpenAsync(new CancellationToken());
                    task.Wait();
                }
                catch(TTransportException e){
                    //TODO, should define our own Exception
                    // here we just print the exception
                    Console.Write(e);
                    throw e;
                }
            }
            if(enableRPCCompression){
                client = new TSIService.Client(new TCompactProtocol(transport));
            }else{
                client = new TSIService.Client(new TBinaryProtocol(transport));
            }
            // init open request
            var open_req = new TSOpenSessionReq(protocol_version, zoneId);
            open_req.Username = username;
            open_req.Password = password;
            try{
                var task = client.openSessionAsync(open_req);
                task.Wait();
                var open_resp = task.Result;
                if(open_resp.ServerProtocolVersion != protocol_version){
                    var message = String.Format("Protocol Differ, Client version is {0} but Server version is {1}", protocol_version, open_resp.ServerProtocolVersion);
                    Console.WriteLine(message);
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
            catch(Exception e){
                transport.Close();
                Console.WriteLine("session closed because ", e);
                throw e;
            }
            if(zoneId != ""){
                set_time_zone(zoneId);
            }else{
                zoneId = get_time_zone();
            } 
            is_close = false;          

        }
        public bool is_open(){
            return !is_close;
        }
        public void close(){
            if(is_close){
                return;
            }
            var req = new TSCloseSessionReq(sessionId);
            try{
                var task = client.closeSessionAsync(req);
                task.Wait();
            }
            catch(TException e){
                var message = String.Format("Error occurs when closing session at server. Maybe server is down. Error message:{0}", e);
                Console.WriteLine(message);
                throw e;
            }
            finally{
                is_close = true;
                if (transport != null){
                    transport.Close();
                }
            }

        }
        public int set_storage_group(string group_name){
            TSStatus status;
            try{
                var task = client.setStorageGroupAsync(sessionId, group_name);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("set storage group {0} failed, beacuse {1}", group_name, e);
                Console.WriteLine(err_msg);
                throw e;
            }
            var message = String.Format("set storage group {0} message: {1}", group_name, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }

        public int delete_storage_group(string group_name){
            TSStatus status;
            try{
                var task = client.deleteStorageGroupsAsync(sessionId, new List<string>{group_name});
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("delete storage group {0} failed, beacuse {1}", group_name, e);
                Console.WriteLine(err_msg);
                throw e;
            }
            var message = String.Format("delete storage group {0} message: {1}", group_name, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int delete_storage_groups(List<string> group_names){
            TSStatus status;
            try{
                var task = client.deleteStorageGroupsAsync(sessionId, group_names);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("delete storage group(s) {0} failed, beacuse {1}", group_names, e);
                Console.WriteLine(err_msg);
                throw e;                
            }
            var message = String.Format("delete storage group(s) {0} message: {1}", group_names, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }

        public int create_time_series(string ts_path, TSDataType data_type, TSEncoding encoding, Compressor compressor){
            TSStatus status;
            var req = new TSCreateTimeseriesReq(sessionId, ts_path, (int)data_type, (int)encoding, (int)compressor);
            try{
                var task = client.createTimeseriesAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("create time series {0} failed, beacuse {1}", ts_path, e);
                Console.WriteLine(err_msg);
                throw e;
            }
            var message = String.Format("creating time series {0} message: {1}", ts_path, status.Message);
            Console.WriteLine(message);
            return verify_success(status); 
        }

        public int create_multi_time_series(List<string> ts_path_lst, List<TSDataType> data_type_lst, List<TSEncoding> encoding_lst, List<Compressor> compressor_lst){
            var data_types = data_type_lst.ConvertAll<int>(x => (int)x);
            var encodings = encoding_lst.ConvertAll<int>(x => (int)x);
            var compressors = compressor_lst.ConvertAll<int>(x => (int)x);
            TSStatus status;
            var req = new TSCreateMultiTimeseriesReq(sessionId, ts_path_lst, data_types, encodings, compressors);
            try{
                var task = client.createMultiTimeseriesAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("create multiple time series {0} failed, beacuse {1}", ts_path_lst, e);
                Console.WriteLine(err_msg);
                throw e;             
            }
            var message = String.Format("creating multiple time series {0} message: {1}", ts_path_lst, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int delete_time_series(List<string> path_list){
            TSStatus status;
            try{
                var task = client.deleteTimeseriesAsync(sessionId, path_list);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var err_msg = String.Format("delete time series {0} failed, beacuse {1}", path_list, e);
                Console.WriteLine(err_msg);
                throw e;             
            }
            var message = String.Format("deleting multiple time series {0} message: {1}", path_list, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int delete_time_series(string ts_path){
            return delete_time_series(new List<string>{ts_path});
        }
        public bool check_time_series_exists(string ts_path){
            // TBD by dalong
            try{
                string sql = "SHOW TIMESERIES " + ts_path;
                return execute_query_statement(sql).has_next();
            }
            catch(TException e){
                var err_msg = String.Format("could not check if certain time series exists because {0}", e);
                Console.WriteLine(err_msg);
                throw e;
            }
        }
        public int delete_data(List<string> ts_path_lst, long start_time, long end_time){
            var req = new TSDeleteDataReq(sessionId, ts_path_lst, start_time, end_time);
            TSStatus status;
            try{
                var task = client.deleteDataAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("data deletion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("delete data from {0}, message: {1}", ts_path_lst, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public TSInsertStringRecordReq gen_insert_str_record_req(string device_id, List<string> measurements, List<string> values, long timestamp){
            if(values.Count() != measurements.Count()){
                var err_msg = "length of data types does not equal to length of values!";
                throw new TException(err_msg, null);
            }
            return new TSInsertStringRecordReq(sessionId, device_id, measurements, values, timestamp);
        }
        public int insert_record(string device_id, List<string> measurements, List<string> values, long timestamp){
            // TBD by Luzhan
            var req = gen_insert_str_record_req(device_id, measurements, values, timestamp);
            Console.WriteLine(req);
            TSStatus status;
            try{
                var task = client.insertStringRecordAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("String insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("insert one record to device {0} message: {1}", device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public TSInsertRecordReq gen_insert_record_req(string device_id, List<string> measurements, List<string> values, List<TSDataType> data_types, long timestamp){
            if(values.Count() != data_types.Count() || values.Count() != measurements.Count()){
                var err_msg = "length of data types does not equal to length of values!";
                throw new TException(err_msg, null);
            }
            var values_in_bytes = value_to_bytes(data_types, values);
            return new TSInsertRecordReq(sessionId, device_id, measurements, values_in_bytes, timestamp);
        }
        public int insert_record(string device_id, List<string> measurements, List<string> values, List<TSDataType> data_types, long timestamp){
            // TBD by Luzhan
            var req = gen_insert_record_req(device_id, measurements, values, data_types, timestamp);
            TSStatus status;
            try{
                var task = client.insertRecordAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Record insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("insert one record to device {0} message: {1}", device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public TSInsertRecordsReq gen_insert_records_req(List<string> device_id, List<List<string>> measurements_lst, List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, List<long> timestamp_lst){
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
                var values_in_bytes = value_to_bytes(data_types, values);
                values_lst_in_bytes.Add(values_in_bytes);
            }

            return new TSInsertRecordsReq(sessionId, device_id, measurements_lst, values_lst_in_bytes, timestamp_lst);
        }
        public int insert_records(List<string> device_id, List<List<string>> measurements_lst, List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, List<long> timestamp_lst){
            // TBD by Luzhan
            var req = gen_insert_records_req(device_id, measurements_lst, values_lst, data_types_lst, timestamp_lst);
            TSStatus status;
            try{
                var task = client.insertRecordsAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Multiple records insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("insert multiple records to devices {0} message: {1}", device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int test_insert_record(string device_id, List<string> measurements, List<string> values, List<TSDataType> data_types, long timestamp){
            // TBD by Luzhan
            var req = gen_insert_record_req(device_id, measurements, values, data_types, timestamp);
            TSStatus status;
            try{
                var task = client.testInsertRecordAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Testing record insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("testing! insert one record to device {0} message: {1}", device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int test_insert_records(List<string> device_id, List<List<string>> measurements_lst, List<List<string>> values_lst, List<List<TSDataType>> data_types_lst, List<long> timestamp_lst){
            // TBD by Luzhan
            var req = gen_insert_records_req(device_id, measurements_lst, values_lst, data_types_lst, timestamp_lst);
            TSStatus status;
            try{
                var task = client.testInsertRecordsAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Testing multiple records insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("testing! insert multiple records, message: {0}", status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public TSInsertTabletReq gen_insert_tablet_req(Tablet tablet){
            List<int> data_type_values = new List<int>(){};
            for(int i = 0; i < tablet.data_type_lst.Count(); i++){
                var data_type_value = (int)tablet.data_type_lst[i];
                data_type_values.Add(data_type_value);
            }
            return new TSInsertTabletReq(sessionId, tablet.device_id, tablet.measurement_lst, tablet.get_binary_values(), tablet.get_binary_timestamps(), data_type_values, tablet.row_number);
        }
        public int insert_tablet(Tablet tablet){
            // TBD by Luzhan
            var req = gen_insert_tablet_req(tablet);
            TSStatus status;
            try{
                var task = client.insertTabletAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Tablet insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }            
            var message = String.Format("insert one tablet to device {0} message: {1}", tablet.device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public TSInsertTabletsReq gen_insert_tablets_req(List<Tablet> tablet_lst){
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
            return new TSInsertTabletsReq(sessionId, device_id_lst, measurements_lst, values_lst, timestamps_lst, type_lst, size_lst);
        }

        public int insert_tablets(List<Tablet> tablet_lst){
            // TBD by Luzhan
            var req = gen_insert_tablets_req(tablet_lst);
            TSStatus status;
            try{
                var task = client.insertTabletsAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Multiple tablets insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("insert multiple tablets, message: {0}", status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }
        public int insert_records_of_one_device(long device_id, List<long> timestamp_lst, List<List<string>> measurements_lst, List<List<TSDataType>> data_types_lst, List<List<string>> values_lst){
            var sorted = timestamp_lst.Select((x, index) => (timestamp: x, measurements:measurements_lst[index], data_types:data_types_lst[index], values:values_lst[index])).OrderBy(x => x.timestamp).ToList();
            List<long> sorted_timestamp_lst = sorted.Select(x => x.timestamp).ToList();
            List<List<string>> sorted_measurements_lst = sorted.Select(x => x.measurements).ToList();
            List<List<TSDataType>> sorted_datatye_lst = sorted.Select(x => x.data_types).ToList();
            List<List<string>> sorted_value_lst = sorted.Select(x => x.values).ToList();
            return insert_records_of_one_device_sorted(device_id, sorted_timestamp_lst, sorted_measurements_lst, sorted_datatye_lst, sorted_value_lst);

        }
        public bool check_sorted(List<long> timestamp_lst){
            for(int i = 1; i < timestamp_lst.Count(); i++){
                if(timestamp_lst[i] < timestamp_lst[i-1]){
                    return false;
                }
            }
            return true;
        }
        public TSInsertRecordsOfOneDeviceReq gen_insert_records_of_one_device_request(long device_id, List<long> timestamp_lst, List<List<string>> measurements_lst,  List<List<string>> values_lst, List<List<TSDataType>> data_types_lst){
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
                var value_in_bytes = value_to_bytes(data_type_values, values_lst[i]);
                binary_value_lst.Add(value_in_bytes);
            }
            return new TSInsertRecordsOfOneDeviceReq(sessionId, device_id.ToString(), measurements_lst, binary_value_lst, timestamp_lst);
        }
        public int insert_records_of_one_device_sorted(long device_id, List<long> timestamp_lst, List<List<string>> measurements_lst, List<List<TSDataType>> data_types_lst, List<List<string>> values_lst){
            // TBD by Luzhan
            var size = timestamp_lst.Count();
            if(size != measurements_lst.Count() || size != data_types_lst.Count() || size != values_lst.Count()){
                var err_msg = "insert records of one device error: types, times, measurementsList and valuesList's size should be equal";
                throw new TException(err_msg, null);
            }
            if(!check_sorted(timestamp_lst)){
                var err_msg = "insert records of one device error: timestamp not sorted";
                throw new TException(err_msg, null);
            }
            var req = gen_insert_records_of_one_device_request(device_id, timestamp_lst, measurements_lst, values_lst, data_types_lst);
            TSStatus status;
            try{
                var task = client.insertRecordsOfOneDeviceAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Sorted records of one device insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("insert records of one device, message: {0}", status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }

        public int test_insert_tablet(Tablet tablet){
            // TBD by Luzhan
            var req = gen_insert_tablet_req(tablet);
            TSStatus status;
            try{
                var task = client.testInsertTabletAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Testing tablet insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("testing! insert one tablet to device {0} message: {1}", tablet.device_id, status.Message);
            Console.WriteLine(message);
            return verify_success(status);            
        }
        public int test_insert_tablets(List<Tablet> tablet_lst){
            // TBD by Luzhan
            var req = gen_insert_tablets_req(tablet_lst);
            TSStatus status;
            try{
                var task = client.testInsertTabletsAsync(req);
                task.Wait();
                status = task.Result;
            }
            catch(TException e){
                var message_local = String.Format("Testing multiple tablets insertion fails because: {0}", e);
                Console.WriteLine(message_local);
                throw e;
            }
            var message = String.Format("testing! insert multiple tablets, message: {0}", status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }

        private int verify_success(TSStatus status){
            if (status.Code == SUCCESS_CODE){
                return 0;
            }

            var message = String.Format("error status is {0}", status);
            Console.WriteLine(message);
            return -1;
        }

        // 这个函数可能会用到，C++里面验证成功的时候不是返回值，而是throw一个exception
        private void verify_success_(TSStatus status){
            if(status.Code == SUCCESS_CODE){
                return;
            }
            var message = String.Format("error status is {0}, {1}", status.Code, status.Message);
            Console.WriteLine(message);
            throw new TException();
        }
         
        public void set_time_zone(string zoneId){
            var req = new TSSetTimeZoneReq(sessionId, zoneId);
            try{
                var task = client.setTimeZoneAsync(req);
                task.Wait();
                var message = String.Format("setting time zone_id as {0}, message:{1}", zoneId, task.Result.Message);
                Console.WriteLine(message);
            }
            catch(TException e ){
                var message = String.Format("could not set time zone because {0}", e);
                Console.WriteLine(message);
                throw e; 
            }
            this.zoneId = zoneId;
        }
        public string get_time_zone(){
            TSGetTimeZoneResp resp;
            if(zoneId != ""){
                return zoneId;
            }
            try{
                var task = client.getTimeZoneAsync(sessionId);
                task.Wait();
                resp = task.Result;
            }
            catch(TException e){
                var message = String.Format("counld not get time zone beacuse {0}", e);
                Console.WriteLine(message);
                throw e; 
            }
            return resp.TimeZone;
        }
        public SessionDataSet execute_query_statement(string sql){
            TSExecuteStatementResp resp;
            TSStatus status;
            var req = new TSExecuteStatementReq(sessionId, sql, statementId);
            req.FetchSize = this.fetch_size;
            try{
                var task = client.executeQueryStatementAsync(req);
                task.Wait();
                resp = task.Result;
                status = resp.Status;
            }
            catch(TException e){
                var err_msg = String.Format("could not execute query statement because {0}", e);
                Console.WriteLine(err_msg);
                throw e;
            }
            if(verify_success(status) == -1){
                throw new TException("execute query failed", null);
            }
            return new SessionDataSet(sql, resp.Columns, resp.DataTypeList, resp.ColumnNameIndexMap, resp.QueryId, client, sessionId, resp.QueryDataSet);

        }
        public int execute_non_query_statement(string sql){
            TSExecuteStatementResp resp;
            TSStatus status;
            var req = new TSExecuteStatementReq(sessionId, sql, statementId);
            try{
                var task = client.executeUpdateStatementAsync(req);
                task.Wait();
                resp = task.Result;
                status = resp.Status;
            }
            catch(TException e){
                var err_msg = String.Format("execution of non-query statement fails because: {0}", e);
                Console.WriteLine(err_msg);
                throw e;
            }
            var message = String.Format("execute non-query statement {0} message: {1}", sql, status.Message);
            Console.WriteLine(message);
            return verify_success(status);
        }


        public byte[] value_to_bytes(List<TSDataType> data_types, List<string> values){

            ByteBuffer buffer = new ByteBuffer(new byte[]{});
            for(int i = 0;i < data_types.Count(); i++){
                buffer.add_char((char)data_types[i]);
                switch(data_types[i]){
                    case TSDataType.BOOLEAN:
                        buffer.add_bool(bool.Parse(values[i]));
                        break;
                    case TSDataType.INT32:
                        buffer.add_int(int.Parse(values[i]));
                        break;
                    case TSDataType.INT64:
                        buffer.add_long(long.Parse(values[i]));
                        break;
                    case TSDataType.FLOAT:
                        buffer.add_float(float.Parse(values[i]));
                        break;
                    case TSDataType.DOUBLE:
                        buffer.add_double(double.Parse(values[i]));
                        break;
                    case TSDataType.TEXT:
                        buffer.add_str(values[i]);
                        break;
                    default:
                        var message = String.Format("Unsupported data type:{0}",data_types[i]);
                        Console.WriteLine(message);
                        break;
                }
            }
            // Append value into buffer in Big Endian order to comply with IoTDB server
            var buf = buffer.get_buffer();
            
            //if(BitConverter.IsLittleEndian){
            //    buf = buf.Reverse().ToArray();
            //}
            return buf;
        }
    }
}
