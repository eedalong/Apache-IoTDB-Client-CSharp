using System.Collections.Generic;
using iotdb_client_csharp.client;
using System;
namespace iotdb_client_csharp.client.utils
{
    public class ColumnRecord<T>
    {
     public TSDataType data_type;
     List<T> values;

     public ColumnRecord(TSDataType data_type, List<T> values){
         this.data_type = data_type;
         this.values = values;
     }
     public byte[] get_value_bytes(){
        List<byte> res = new List<byte>{};
         foreach(var value in values){
             switch(data_type){
                case TSDataType.BOOLEAN:
                    bool bool_val = (bool)(object)value;
                    res.AddRange(BitConverter.GetBytes(bool_val));
                    break;
                case TSDataType.FLOAT:
                    float float_val = (float)(object)value;
                    res.AddRange(BitConverter.GetBytes(float_val));
                    break;
                case TSDataType.DOUBLE:
                    double double_val = (double)(object)value;
                    res.AddRange(BitConverter.GetBytes(double_val));
                    break;
                case TSDataType.INT32:
                    int int_val = (int)(object)value;
                    res.AddRange(BitConverter.GetBytes(int_val));
                    break;
                case TSDataType.INT64:
                    long long_val = (long)(object)value;
                    res.AddRange(BitConverter.GetBytes(long_val));
                    break;
                case TSDataType.TEXT:
                    string str_val = (string)(object)(value);
                    res.AddRange(BitConverter.GetBytes(str_val.Length));
                    res.AddRange(System.Text.Encoding.UTF8.GetBytes(str_val));
                    break;
             }
         }
        return res.ToArray();

     }

    }
}