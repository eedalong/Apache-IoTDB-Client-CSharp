using System;
using System.Collections.Generic;
using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class SessionDataSet
    {
        private long session_id, query_id;
        private string sql;
        List<string> column_name_lst;
        Dictionary<string, int> column_name_index_map;
        Dictionary<int, int> duplicate_location;
        List<string> deduplicated_column_type_lst;
        TSQueryDataSet query_dataset;
        byte[] current_bitmap;
        int column_size;
        List<ByteBuffer> value_buffer_lst, bitmap_buffer_lst;
        ByteBuffer time_buffer;
        TSIService.Client client;

        private string TIMESTAMP_STR{
            get{return "Time";}
        }
        private int START_INDEX{
            get{return 2;}
        }
        private int FLAG{
            get{return 0x80;}
        }
        private int default_timeout{
            get{return 10000;}
        }
        public int fetch_size{get;set;}
        private int row_index;

        private bool has_catched_result, empty_result;
        private RowRecord cached_row_record;

        

        public SessionDataSet(string sql, List<string> column_name_lst, List<string> column_type_lst, Dictionary<string, int> column_name_index, long query_id, TSIService.Client client, long session_id, TSQueryDataSet query_data_set){
            this.sql = sql;
            this.query_dataset = query_data_set;
            this.query_id = query_id;
            this.current_bitmap = new byte[column_name_lst.Count];
            this.column_size = column_name_lst.Count;
            this.column_name_lst = column_name_lst;
            this.column_size = column_name_lst.Count;
            this.time_buffer = new ByteBuffer(query_data_set.Time);
            this.client = client;

            // some internal variable
            has_catched_result = false;
            row_index = 0;
        
            for(int index = 0; index < column_name_lst.Count; index++){
                var column_name = column_name_lst[index];
                if(this.column_name_index_map.ContainsKey(column_name)){
                    this.duplicate_location[index] = this.column_name_index_map[column_name];
                }else{
                    this.column_name_index_map[column_name] = index;
                    this.deduplicated_column_type_lst.Add(column_type_lst[index]);
                }
                this.value_buffer_lst.Add(new ByteBuffer(query_data_set.ValueList[index]));
                this.bitmap_buffer_lst.Add(new ByteBuffer(query_data_set.BitmapList[index]));
            }


        }
        
        public List<string> get_column_names(){
            return this.column_name_lst;
        }
        public bool has_next(){
            if(has_catched_result){
                return true;
            }
            // we have consumed all current data, fetch some more
            if(!this.time_buffer.has_remaining()){
                if(!fetch_results()){
                    return false;
                }
            }
            construct_one_row();
            has_catched_result = true;
            return true;
        }
        public RowRecord next(){
            if(!has_catched_result){
                if(!has_next()){
                    return null;
                }
            }
            has_catched_result = false;
            return cached_row_record;
        }
        private TSDataType get_data_type_from_str(string str){
            switch(str){
                case "BOOLEAN":
                    return TSDataType.BOOLEAN;
                case "INT32":
                    return TSDataType.INT32;
                case "INT64":
                    return TSDataType.INT64;
                case "FLOAT":
                    return TSDataType.FLOAT;
                case "DOUBLE":
                    return TSDataType.DOUBLE;
                case "TEXT":
                    return TSDataType.TEXT;
                case "NULLTYPE":
                    return TSDataType.NONE;
                default:
                    return TSDataType.TEXT;
            }
        }
        public void construct_one_row(){
            List<Field> field_lst = new List<Field>{};
            int loc = 0;
            for(int i = 0; i < this.column_size; i++){
                if(duplicate_location.ContainsKey(i)){
                    var field = field_lst[duplicate_location[i]];
                    field_lst.Add(field);
                }else{
                    ByteBuffer column_value_buffer = value_buffer_lst[loc];
                    ByteBuffer column_bitmap_buffer = bitmap_buffer_lst[loc];
                    if(row_index % 8 == 0){
                        current_bitmap[loc] = column_bitmap_buffer.get_byte();
                    }
                    if(!is_null(loc, row_index)){
                        TSDataType column_data_type = get_data_type_from_str(deduplicated_column_type_lst[loc]);
                        var local_field = new Field(column_data_type);
                        switch(column_data_type){
                            case TSDataType.BOOLEAN:
                                var bool_val = column_value_buffer.get_bool();
                                local_field.set(local_field);
                                break;
                            case TSDataType.INT32:
                                var int_val = column_value_buffer.get_int();
                                local_field.set(int_val);
                                break;
                            case TSDataType.INT64:
                                var long_val = column_value_buffer.get_long();
                                local_field.set(long_val);
                                break;
                            case TSDataType.FLOAT:
                                float float_val = column_value_buffer.get_float();
                                local_field.set(float_val);
                                break;
                            case TSDataType.DOUBLE:
                                double double_val = column_value_buffer.get_double();
                                local_field.set(double_val);
                                break;
                            case TSDataType.TEXT:
                                string str_val = column_value_buffer.get_str();
                                local_field.set(str_val);
                                break;
                            default:
                                string err_msg = string.Format("value format not supported");
                                Console.WriteLine(err_msg);
                                throw new TException(err_msg, null);
                        }
                        field_lst.Add(local_field);

                    }
                    else{
                        var local_field = new Field(TSDataType.NONE);
                        field_lst.Add(local_field);
                    }
                    loc += 1;
                }
            }
            long timestamp = time_buffer.get_long();
            row_index += 1;
            this.cached_row_record = new RowRecord(timestamp, field_lst);
        }
        private bool is_null(int loc, int row_index){
            byte bitmap = current_bitmap[loc];
            int shift = row_index % 8;
            return ((FLAG >> shift) & bitmap) == 0;

        }
        private bool fetch_results(){
            row_index = 0;
            var req = new TSFetchResultsReq(session_id, sql, fetch_size, query_id, true);
            req.Timeout = default_timeout;
            try{
                var task = client.fetchResultsAsync(req);
                task.Wait();
                var resp = task.Result;
                //TODO we should check response status
                if(resp.Status.Code != 200){
                    throw new TException("fetch result failed", null);
                }
                if(resp.HasResultSet){
                    this.query_dataset = resp.QueryDataSet;
                    // reset buffer
                    this.time_buffer = new ByteBuffer(resp.QueryDataSet.Time);
                    this.value_buffer_lst = new List<ByteBuffer>{};
                    this.bitmap_buffer_lst = new List<ByteBuffer>{};
                    for(int index = 0; index < query_dataset.ValueList.Count; index++){
                        this.value_buffer_lst.Add(new ByteBuffer(query_dataset.ValueList[index]));
                        this.bitmap_buffer_lst.Add(new ByteBuffer(query_dataset.BitmapList[index]));
                    }
                    // reset row index
                    row_index = 0;
                }
                return resp.HasResultSet;
            }
            catch(TException e){
                var message = string.Format("Cannot fetch result from server, because of network connection:{0}", e);
                Console.WriteLine(message);
                throw e;
            }
        }
        public void close_operation_handle(){
            var req = new TSCloseOperationReq(session_id);
            req.QueryId = query_id;
            try{
                var task = client.closeOperationAsync(req);
                task.Wait();
                var status = task.Result;
                var message = string.Format("close session {0}, message: {1}", session_id, status.Message);
                Console.WriteLine(message);
            }
            catch(TException e){
                var message = string.Format("close session {0} failed because:{1} ", session_id, e);
                Console.WriteLine(message);
                throw e;
            }
            
            
        }
    }
}

        