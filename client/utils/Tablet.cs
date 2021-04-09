using System;
using System.Collections.Generic;
using System.Linq;
namespace iotdb_client_csharp.client.utils
{
    /*
    * A tablet data of one device, the tablet contains multiple measurements of this device that share
    * the same time column.
    *
    * for example:  device root.sg1.d1
    *
    * time, m1, m2, m3
    *    1,  1,  2,  3
    *    2,  1,  2,  3
    *    3,  1,  2,  3
    *
    * Notice: The tablet should not have empty cell
    *
    */
    public class Tablet
    {
       public string device_id{get;}
       public List<string> measurement_lst{get;}
       public List<TSDataType> data_type_lst{get;}
       private List<long> timestamp_lst;
       private List<List<string>> value_lst;

       public int row_number{get;}
       private int col_number;

       public Tablet(string device_id, List<string> measurement_lst, List<TSDataType> data_type_lst, List<List<string>> value_lst, List<long> timestamp_lst){
            if(timestamp_lst.Count != value_lst.Count){
                var err_msg = String.Format("Input error! len(timestamp_lst) does not equal to len(value_lst)!");
                Console.WriteLine(err_msg);
                throw new Exception("input length not matched");
            }
            if(measurement_lst.Count != data_type_lst.Count){
                var err_msg = string.Format("Input Error, len(measurement_lst) does not equal to len(data_type_lst)");
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
           ByteBuffer buffer = new ByteBuffer(new byte[]{});
           foreach(var timestamp in timestamp_lst){
               buffer.add_long(timestamp);
           }
           return buffer.get_buffer();
       }
       
       public byte[] get_binary_values(){
           ByteBuffer buffer = new ByteBuffer(new byte[]{});
           for(int i = 0; i < col_number; i++){
                switch(data_type_lst[i]){
                    case TSDataType.BOOLEAN:
                        for(int j=0; j< row_number; j++){
                            buffer.add_bool(bool.Parse(value_lst[j][i]));
                        }
                        break;
                    case TSDataType.INT32:
                        for(int j=0; j<row_number; j++){
                            buffer.add_int(int.Parse(value_lst[j][i]));
                        }
                        break;
                    case TSDataType.INT64:
                        for(int j=0; j<row_number; j++){
                            buffer.add_long(long.Parse(value_lst[j][i]));
                        }
                        break;
                    case TSDataType.FLOAT:
                        for(int j=0; j<row_number; j++){
                            buffer.add_float(float.Parse(value_lst[j][i]));
                        }
                        break;
                    case TSDataType.DOUBLE:
                        for(int j=0; j<row_number; j++){
                            buffer.add_double(double.Parse(value_lst[j][i]));
                        }
                        break;
                    case TSDataType.TEXT:
                        for(int j=0; j<row_number; j++){
                            buffer.add_str(value_lst[j][i]);
                        }
                        break;
                    default:
                        var message = String.Format("Unsupported data type {0}", data_type_lst[i]);
                        Console.WriteLine(message);
                        break;
                }
           }
           return buffer.get_buffer();
       }




    }
}