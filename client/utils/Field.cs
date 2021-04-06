using System;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class Field
    {
        // TBD By Zengz
        public bool bool_val{
            get{
                return bool_val;
            }
            set{
                valid_data_type();
                bool_val = value;
            }
        }
        public int int_val{
            get{
                return int_val;
            }
            set{
                valid_data_type();
                int_val = value;
            }
        }
        public long long_val{
            get{
                return long_val;
            }
            set{
                valid_data_type();
                long_val = value;
            }
        }
        public float float_val{
            get{
                return float_val;
            }
            set{
                valid_data_type();
                float_val = value;
            }
        }
        public double double_val{
            get{
                return double_val;
            }
            set{
                valid_data_type();
                double_val = value;
            }
        }
        // why we need to keep binary values here 
        private byte[] binary_val{
            get{
                return binary_val;
            }
            set{
                binary_val = value;
            }
        }


        private TSDataType data_type{get;set;}
        
        public Field(TSDataType data_type){
            this.data_type = data_type;
        }
        public void valid_data_type(){
            if(data_type == TSDataType.NONE){
                throw new Exception("CanNot Set A None Type");
            }
        }

        public void set_value<T>(T value){
            switch(data_type){
                case TSDataType.BOOLEAN:
                    bool_val = (bool)(object)value;
                    break;
                case TSDataType.INT32:
                    int_val = (int)(object)value;
                    break;
                case TSDataType.INT64:
                    long_val = (long)(object)value;
                    break;
                case TSDataType.FLOAT:
                    float_val= (float)(object)value;
                    break;
                case TSDataType.DOUBLE:
                    double_val = (double)(object)value;
                    break;
                case TSDataType.TEXT:
                    var res = new List<byte>{};
                    var str_val = (string)(object)value;
                    res.AddRange(BitConverter.GetBytes(str_val.Length));
                    res.AddRange(System.Text.Encoding.UTF8.GetBytes(str_val));
                    binary_val = res.ToArray();
                    break;
                default:
                    var message = "unsupported data type";
                    Console.WriteLine(message);
                    break;
            }
        }
        public byte[] get_value_bytes(){
            List<byte> res = new List<byte>{};
            switch(data_type){
                case TSDataType.BOOLEAN:
                    res.AddRange(BitConverter.GetBytes(bool_val));
                    break;
                case TSDataType.INT32:
                    res.AddRange(BitConverter.GetBytes(int_val));
                    break;
                case TSDataType.INT64:
                    res.AddRange(BitConverter.GetBytes(long_val));
                    break;
                case TSDataType.FLOAT:
                    res.AddRange(BitConverter.GetBytes(float_val));
                    break;
                case TSDataType.DOUBLE:
                    res.AddRange(BitConverter.GetBytes(double_val));
                    break;
                case TSDataType.TEXT:
                    res.AddRange(binary_val);
                    break;
            }
            return res.ToArray();
        }

        public override string ToString()
        {
            switch(data_type){
                case TSDataType.TEXT:
                    return System.Text.Encoding.UTF8.GetString(binary_val);
                case TSDataType.INT32:
                    return int_val.ToString();
                case TSDataType.INT64:
                    return long_val.ToString();
                case TSDataType.FLOAT:
                    return float_val.ToString();
                case TSDataType.DOUBLE:
                    return double_val.ToString();
                default:
                    return "";
            }
        }

    }
}