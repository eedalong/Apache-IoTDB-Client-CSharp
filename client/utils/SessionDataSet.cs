using System;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class SessionDataSet
    {
        private IoTDBRpcDataSet iotdb_rpc_data_set;
        public SessionDataSet(string sql, List<string> column_name_lst, List<string> column_type_lst, Dictionary<string, int> column_name_index, long query_id, TSIService.Client client, long session_id, TSQueryDataSet query_data_set, bool ignore_timestamp){
            iotdb_rpc_data_set = new IoTDBRpcDataSet(sql, column_name_lst, column_type_lst, column_name_index,ignore_timestamp, query_id, client, session_id, query_data_set, 1024);
        }
        public int get_fetch_size(){
            return iotdb_rpc_data_set.get_fetch_size();
        }
        public int set_fetch_size(int fetch_size){
            return iotdb_rpc_data_set.set_fetch_size(fetch_size);
        }
        public List<string> get_column_names(){
            return iotdb_rpc_data_set.get_column_names();
        }
        public List<TSDataType> get_column_types(){
            return iotdb_rpc_data_set.get_column_types();
        }
        public bool has_next(){
            return iotdb_rpc_data_set.next();
        }
        public void close_operation_handle(){
            iotdb_rpc_data_set.close();
        }
        private bool unpack_bool(byte[] value_byes){
            return false;
        }
        private int unpack_int(byte[] value_byes){
            return 0;
        }
        private long unpack_long(byte[] value_bytes){
            return 0;
        }
        private float unpack_float(byte[] value_bytes){
            return 0;
        }

        private double unpack_double(byte[] value_bytes){
            return 0;
        }
        public RowRecord next(){
            if(!(iotdb_rpc_data_set.get_has_cached_record() || has_next())){
                return null;
            }
            iotdb_rpc_data_set.set_has_cached_record(false);
            return construct_row_record_from_value_array();
        }

        public RowRecord construct_row_record_from_value_array(){
            var out_field_lst = new List<Field>{};
            for(int i = 0; i < iotdb_rpc_data_set.get_column_size(); i++){
                var index = i + 1;
                var dataset_column_index = i + iotdb_rpc_data_set.START_INDEX;
                if(iotdb_rpc_data_set.get_ignore_timestamp()){
                    index -= 1;
                    dataset_column_index -= 1;
                }
                Field field;
                var column_name = iotdb_rpc_data_set.get_column_names()[index];
                var location = iotdb_rpc_data_set.get_column_ordinal_dict()[column_name] - iotdb_rpc_data_set.START_INDEX;
                if(!iotdb_rpc_data_set.is_null_by_index(dataset_column_index)){
                    var value_bytes = iotdb_rpc_data_set.get_values()[location];
                    var data_type = iotdb_rpc_data_set.get_column_type_deduplicated_list()[location];
                    field = new Field(data_type);
                    
                    switch(data_type){
                        case TSDataType.BOOLEAN:
                            var bool_value = unpack_bool(value_bytes);
                            field.set_value(bool_value);
                            break;
                        case TSDataType.INT32:
                            var int_value = unpack_int(value_bytes);
                            field.set_value(int_value);
                            break;
                        case TSDataType.INT64:
                            var long_value = unpack_long(value_bytes);
                            field.set_value(long_value);
                            break;
                        case TSDataType.TEXT:
                            var bytes_value = value_bytes;
                            field.set_value(bytes_value);
                            break;
                        case TSDataType.FLOAT:
                            float float_value = unpack_float(value_bytes);
                            field.set_value(float_value);
                            break;
                        case TSDataType.DOUBLE:
                            double double_value = unpack_double(value_bytes);
                            field.set_value(double_value);
                            break;
                        default:
                            var message = "unsupported data type";
                            Console.WriteLine(message);
                            break;
                    }

                }else{
                    field = new Field(TSDataType.NONE);
                }
                out_field_lst.Add(field);
            }
            long timestamp = unpack_long(iotdb_rpc_data_set.get_time_bytes());
            return new RowRecord(timestamp, out_field_lst);
        }

    }
}