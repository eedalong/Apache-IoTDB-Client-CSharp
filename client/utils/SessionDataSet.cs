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

    }
}