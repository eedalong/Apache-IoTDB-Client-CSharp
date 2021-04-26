using System.Collections.Generic;
using System;
using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class Utils
    {
        public bool check_sorted(List<long> timestamp_lst){
            for(int i = 1; i < timestamp_lst.Count; i++){
                if(timestamp_lst[i] < timestamp_lst[i-1]){
                    return false;
                }
            }
            return true;
        }
        public int verify_success(TSStatus status, int SUCCESS_CODE){
            if(status.__isset.subStatus){
                foreach(var sub_status in status.SubStatus){
                    if(verify_success(sub_status, SUCCESS_CODE) != 0){
                        return -1;
                    }
                }
                return 0;
            }
            if (status.Code == SUCCESS_CODE){
                return 0;
            }            
            return -1;
        }
        public byte[] value_to_bytes(List<TSDataType> data_types, List<string> values){

            ByteBuffer buffer = new ByteBuffer(values.Count);
            for(int i = 0;i < data_types.Count; i++){
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
                        throw new TException(message, null);
                }
            }
            var buf = buffer.get_buffer();
            return buf;
        }
        public byte[] value_to_bytes(List<object> values){
            // todo by Luzhan

            ByteBuffer buffer = new ByteBuffer(values.Count);
            foreach(var value in values){
                if(value.GetType().Equals(typeof(bool))){
                    buffer.add_bool((bool)value);
                }
                else if((value.GetType().Equals(typeof(Int32)))){

                }
                else if((value.GetType().Equals(typeof(Int64)))){

                }
                else if((value.GetType().Equals(typeof(double)))){

                }
                else if((value.GetType().Equals(typeof(float)))){

                }
                else if((value.GetType().Equals(typeof(string)))){

                }
                else{
                        var message = String.Format("Unsupported data type:{0}",value.GetType().ToString());
                        throw new TException(message, null);
                }
            }
            var buf = buffer.get_buffer();
            return buf;
        }
    }
}