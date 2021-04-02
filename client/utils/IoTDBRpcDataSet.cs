using iotdb_client_csharp.client;
using System;
using System.Collections.Generic;
using Thrift; 
namespace iotdb_client_csharp.client.utils
{
    public class IoTDBRpcDataSet
    {
        public string TIMESTAMP_STR{
            get{return "Time";}
        }
        public int START_INDEX{
            get{return 2;}
        }
        public int FLAG{
            get{return 0x80;}
        }
        private long session_id, query_id, default_timeout;
        private TSIService.Client client;
        private int fetch_size, column_size, rows_index;
        private List<string> column_name_lst;
        private List<TSDataType> column_type_lst, column_type_deduplicated_lst;
        private Dictionary<string, int> column_name_index, data_type_map;
        private bool ignore_timestamp, is_closed, empty_resultset, has_cached_record;

        private TSQueryDataSet query_dataset;
        private string sql;
        private byte[] time_bytes;
        private List<byte[]> value;
        private List<byte> current_bitmap; 


        public IoTDBRpcDataSet(string sql, List<string> column_name_lst, List<string> column_type_lst, Dictionary<string, int> column_name_index, bool ignore_timestamp, long query_id, TSIService.Client client, long session_id, TSQueryDataSet query_dataset, int fetch_size){
            this.session_id = session_id;
            this.sql = sql;
            this.ignore_timestamp = ignore_timestamp;
            this.client = client;
            this.fetch_size = fetch_size;
            this.column_size = column_name_lst.Count;
            this.default_timeout = 1000;
            this.column_name_lst = new List<string>{};
            this.column_type_lst = new List<TSDataType>{};
            this.column_name_index = new Dictionary<string, int>{};
            this.column_type_deduplicated_lst = new List<TSDataType>{};
            if(!ignore_timestamp){
                this.column_name_lst.Add(this.TIMESTAMP_STR);
                this.column_type_lst.Add(TSDataType.INT64);
                this.column_name_index.Add(this.TIMESTAMP_STR, 1);
            }
            if(column_name_index.Count > 0){
                // init 
                for(int i=0; i < column_name_index.Count; i++){
                    column_type_deduplicated_lst.Add(TSDataType.NONE);
                }
                // init data type map
                for(int i = 0; i < column_name_lst.Count; i++){
                    var name = column_name_lst[i];
                    this.column_name_lst.Add(name);
                    this.column_type_lst.Add((TSDataType)Enum.Parse(typeof(TSDataType), name));
                    if(!this.column_name_index.ContainsKey(name)){
                        var index = column_name_index[name];
                        this.column_name_index[name] = index + START_INDEX;
                        this.column_type_deduplicated_lst[i] = (TSDataType)Enum.Parse(typeof(TSDataType), name);
                    }
                }
            }else{
                var index = START_INDEX;
                for(int i=0; i < column_name_lst.Count; i++){
                    var name = column_name_lst[i];
                    this.column_name_lst.Add(name);
                    this.column_type_lst.Add((TSDataType)Enum.Parse(typeof(TSDataType), name));
                    if(!this.column_name_index.ContainsKey(name)){
                        this.column_name_index[name] = index;
                        index += 1;
                        this.column_type_deduplicated_lst[index] = (TSDataType)Enum.Parse(typeof(TSDataType), name);

                    }
                }
            }
            time_bytes = new byte[]{};
            for(int i=0; i< column_type_deduplicated_lst.Count; i++){
                current_bitmap.Add((byte)0);
                value.Add(new byte[]{});
            }
            this.query_dataset = query_dataset;
            this.is_closed = false;
            this.has_cached_record = false;
            this.empty_resultset = false;
            this.rows_index = 0;
        }
        public void close(){
            if(is_closed){
                return; 
            }
            if(client != null){
                var req = new TSCloseOperationReq(session_id);
                req.QueryId = query_id;
                var task = client.closeOperationAsync(req);
                task.Wait();
                var status = task.Result;
                try{
                    var message = string.Format("close session {0}, message: {1}", session_id, status.Message);
                    Console.WriteLine(message);
                }
                catch(TException e){
                    var message = string.Format("close session {0} failed because:{1} ", session_id, e);
                    Console.WriteLine(message);
                    throw e;
                }
                is_closed = true;
                client = null;
            }
        }
        public bool next(){
            if(has_cached_result()){
                construct_one_row();
                return true;
            }else if(this.empty_resultset){
                return false;
            }else if(fetch_results()){
                construct_one_row();
                return true;
            }
            return false;
        }
        private bool has_cached_result(){
            return (query_dataset != null) && (query_dataset.Time.Length > 0);
        }
        private bool fetch_results(){
            rows_index = 0;
            var req = new TSFetchResultsReq(session_id, sql, fetch_size, query_id, true);
            req.Timeout = default_timeout;
            try{
                var task = client.fetchResultsAsync(req);
                task.Wait();
                var resp = task.Result;
                if(!resp.HasResultSet){
                    empty_resultset = true;
                }else{
                    this.query_dataset = resp.QueryDataSet;
                }
                return resp.HasResultSet;
            }
            catch(TException e){
                var message = string.Format("Cannot fetch result from server, because of network connection:{0}", e);
                Console.WriteLine(message);
            }
            return false;
        }
        private void construct_one_row(){
            time_bytes = query_dataset.Time[0..8];
            query_dataset.Time = query_dataset.Time[8..];
            for(int index = 0; index < query_dataset.BitmapList.Count; index++){
                var bitmap_buffer = query_dataset.BitmapList[index];
                if(rows_index % 8 == 0){
                    current_bitmap[index] = bitmap_buffer[0];
                    query_dataset.BitmapList[index] = bitmap_buffer[1..];
                }
                if(!is_null(index, rows_index)){
                    var value_buffer = query_dataset.ValueList[index];
                    var data_type = column_type_deduplicated_lst[index];
                    switch(data_type){
                        case TSDataType.BOOLEAN:
                            value[index] = value_buffer[..1];
                            query_dataset.ValueList[index] = value_buffer[1..];
                            break;
                        case TSDataType.INT32:
                           value[index] = value_buffer[..4];
                           query_dataset.ValueList[index] = value_buffer[4..]; 
                           break;
                        case TSDataType.INT64:
                           value[index] = value_buffer[..8];
                           query_dataset.ValueList[index] = value_buffer[8..]; 
                           break; 
                        case TSDataType.TEXT:
                           var length = (Int32)BitConverter.ToUInt32(value_buffer[..4]); 
                           value[index] = value_buffer[4..(4+length)];
                           query_dataset.ValueList[index] = value_buffer[(4+length)..];
                           break;
                        default:
                            Console.WriteLine(string.Format("unsupported data type {0}", data_type));
                            break;
                    }
                }
            }
            rows_index += 1;
            has_cached_record = true;

        }
        private bool is_null(int index, int row_num){
            var bitmap = current_bitmap[index];
            var shift = row_num % 8;
            return ((FLAG >> shift & bitmap) & 0xff ) == 0;
        }
        private bool is_null_by_index(int column_index){
            var name = find_column_name_by_index(column_index);
            var index = column_name_index[name] - START_INDEX;
            if(index < 0){
                return true;
            }
            return is_null(index, rows_index-1);
        }
        private bool is_null_by_name(string column_name){
            var index = column_name_index[column_name] - START_INDEX;
            if(index < 0){
                return true;
            }
            return is_null(index, rows_index-1);
        }

        private string find_column_name_by_index(int column_index){
            if(column_index <= 0){
                throw new Exception("Column index should start from 1");
            }
            if(column_index > column_name_lst.Count){
                throw new Exception(string.Format("column index {0} out of range {0}", column_index, column_name_lst.Count));
            }
            return column_name_lst[column_index -1 ];
        }

        public int get_fetch_size(){
            return fetch_size;
        }
        public int set_fetch_size(int fetch_size){
            this.fetch_size = fetch_size;
            return fetch_size;
        }
        public List<string> get_column_names(){
            return column_name_lst;
        }
        public List<TSDataType> get_column_types(){
            return column_type_lst;
        }
        public int get_column_size(){
            return column_size;
        }
        public bool get_ignore_timestamp(){
            return ignore_timestamp;
        }
        public Dictionary<string, int> get_column_ordinal_dict(){
            return column_name_index;
        }
        public List<TSDataType> get_column_type_deduplicated_list(){
            return column_type_deduplicated_lst;
        }
        public List<byte[]> get_values(){
            return value;
        }
        public byte[] get_time_bytes(){
            return time_bytes;
        }
        public bool get_has_cached_record(){
            return has_cached_record;
        }


        
    }
}