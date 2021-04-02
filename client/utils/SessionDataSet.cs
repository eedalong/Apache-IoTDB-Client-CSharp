using System;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class SessionDataSet
    {
        public SessionDataSet(string sql, List<string> column_name_lst, List<string> column_type_lst, Dictionary<string, int> column_name_index, long query_id, TSIService.Client client, long session_id, TSQueryDataSet query_data_set, bool ignore_time_stamp){
            
        }
    }
}