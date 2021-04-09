using System;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class Field
    {
        // TBD By Zengz
        public bool bool_val;
        public int int_val;
        public long long_val;
        public float float_val;
        public double double_val;
        // why we need to keep binary values here 
        public string str_val;

        private TSDataType type{get;set;}
        
        public Field(TSDataType data_type){
            this.type = data_type;
        }
        public void valid_data_type(){
            if(type == TSDataType.NONE){
                throw new Exception("CanNot Set A None Type");
            }
        }


        public void set(string value){
            str_val = value;
        }
        public void set(int value){
            int_val = value;
        }
        public void set(double value){
            double_val = value;
        }
        public void set(long value){
            long_val = value;
        }
        public void set(float value){
            float_val = value;
        }
        public void set(bool value){
            bool_val = value;
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
                case TSDataType.NONE:
                    var err_msg = string.Format("NONE type does not support get bytes");
                    Console.WriteLine(err_msg);
                    throw new Exception(err_msg);
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
                case TSDataType.BOOLEAN:
                    return bool_val.ToString();
                case TSDataType.NONE:
                    return "NULL";
                default:
                    return "";
            }
        }

    }
}