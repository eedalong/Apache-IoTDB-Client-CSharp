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
        public string str_val{
            get{
                return str_val;
            }
            set{
                str_val = value;
            }
        }


        private TSDataType type{get;set;}
        
        public Field(TSDataType data_type){
            this.type = data_type;
        }
        public void valid_data_type(){
            if(type == TSDataType.NONE){
                throw new Exception("CanNot Set A None Type");
            }
        }

        public void set<T>(T value){
            switch(type){
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
                    str_val = (string)(object)value;
                    break;
                default:
                    var message = "unsupported data type";
                    Console.WriteLine(message);
                    break;
            }
        }
        public T get<T>(){
            switch(type){
                case TSDataType.BOOLEAN:
                    return (T)(object)bool_val;
                case TSDataType.INT64:
                    return (T)(object)long_val;
                case TSDataType.FLOAT:
                    return (T)(object)float_val;
                case TSDataType.INT32:
                    return (T)(object)int_val;
                case TSDataType.DOUBLE:
                    return (T)(object)double_val;
                case TSDataType.TEXT:
                    return (T)(object)str_val;
                default:
                    return (T)(object)null;
            }
        }
        public byte[] get_bytes(){
            ByteBuffer buffer = new ByteBuffer(new byte[]{});
            buffer.add_int((int)type);
            switch(type){
                case TSDataType.BOOLEAN:
                    buffer.add_bool(bool_val);
                    break;
                case TSDataType.INT32:
                    buffer.add_int(int_val);
                    break;
                case TSDataType.INT64:
                    buffer.add_long(long_val);
                    break;
                case TSDataType.FLOAT:
                    buffer.add_float(float_val);
                    break;
                case TSDataType.DOUBLE:
                    buffer.add_double(double_val);
                    break;
                case TSDataType.TEXT:
                    buffer.add_str(str_val);
                    break;
            }
            return buffer.get_buffer();
        }

        public override string ToString()
        {
            switch(type){
                case TSDataType.TEXT:
                    return str_val;
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