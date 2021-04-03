using System;
using System.Collections.Generic;
using System.Linq;
namespace iotdb_client_csharp.client.utils
{
    public class Tablet
    {
        // TBD by Zengz
       public string device_id{get;}
       public List<string> measurement_lst{get;}
       public List<TSDataType> data_type_lst{get;}
       private List<long> timestamp_lst;
       private List<List<string>> value_lst;

       public int row_number{get;}
       private int col_number;

       public Tablet(string device_id, List<string> measurement_lst, List<TSDataType> data_type_lst, List<List<string>> value_lst, List<long> timestamp_lst){
            if(timestamp_lst.Count != value_lst.Count){
                var err_msg = String.Format("Input error! len(timestamps) does not equal to len(values)!");
                Console.WriteLine(err_msg);
                throw new Exception("input length not matched");
            }
            if(!check_timestamp_lst_sorted()){
                var sorted = timestamp_lst.Select((x, index) => (timestamp:x, values:value_lst[index])).OrderBy(x => x.timestamp).ToList();
                this.timestamp_lst = sorted.Select(x => x.timestamp).ToList();
                this.value_lst = sorted.Select(x => x.values).ToList();
            }else{
                this.value_lst = value_lst;
                this.timestamp_lst = timestamp_lst;
            }

           this.device_id = device_id;
           this.measurement_lst = measurement_lst;
           this.data_type_lst = data_type_lst;
           this.row_number = timestamp_lst.Count;
           this.col_number = measurement_lst.Count;
       }

       private bool check_timestamp_lst_sorted(){
           for(int index = 1; index < timestamp_lst.Count; index++){
               if(timestamp_lst[index] < timestamp_lst[index-1]){
                   return false;
               }
           }
           return true ;
       }
       public byte[] get_binary_timestamps(){
           List<byte> res = new List<byte>{};
           foreach(var timestamp in timestamp_lst){
               res.AddRange(BitConverter.GetBytes(timestamp));
           }
           return res.ToArray();
       }
       public byte[] get_binary_values(){
           List<byte> res = new List<byte>{};
           for(int i = 0; i < col_number; i++){
                switch(data_type_lst[i]){
                    case TSDataType.BOOLEAN:
                        for(int j=0; j< row_number; j++){
                            res.AddRange(BitConverter.GetBytes(value_lst[j][i] == "True"));
                        }
                        break;
                    case TSDataType.INT32:
                        for(int j=0; j<row_number; j++){
                            res.AddRange(BitConverter.GetBytes(int.Parse(value_lst[j][i])));
                        }
                        break;
                    case TSDataType.INT64:
                        for(int j=0; j<row_number; j++){
                            res.AddRange(BitConverter.GetBytes(long.Parse(value_lst[j][i])));
                        }
                        break;
                    case TSDataType.FLOAT:
                        for(int j=0; j<row_number; j++){
                            res.AddRange(BitConverter.GetBytes(float.Parse(value_lst[j][i])));
                        }
                        break;
                    case TSDataType.DOUBLE:
                        for(int j=0; j<row_number; j++){
                            res.AddRange(BitConverter.GetBytes(double.Parse(value_lst[j][i])));
                        }
                        break;
                    case TSDataType.TEXT:
                        for(int j=0; j<row_number; j++){
                            res.AddRange(BitConverter.GetBytes(value_lst[j][i].Length));
                            res.AddRange(System.Text.Encoding.UTF8.GetBytes(value_lst[j][i]));
                        }
                        break;
                    default:
                        var message = String.Format("Unsupported data type {0}", data_type_lst[i]);
                        Console.WriteLine(message);
                        break;
                }
           }
           return res.ToArray();
       }




    }
}