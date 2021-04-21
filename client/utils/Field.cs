using System;
using System.Collections.Generic;
using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class Field
    {
        // TBD By Zengz
        private object val;
        public TSDataType type{get;set;}
        public Field(TSDataType data_type){
            this.type = data_type;
        }
        public void set<T>(T value){
            val = value;
        }
        public double get_double(){
            return type==TSDataType.TEXT?double.Parse((string)val):(double)val;
        }
        public Int32 get_int(){
            return type==TSDataType.TEXT?Int32.Parse((string)val):(Int32)val;
        }
        public Int64 get_long(){
            return type==TSDataType.TEXT?Int64.Parse((string)val):(Int64)val;
        }
        public float get_float(){
            return type==TSDataType.TEXT?float.Parse((string)val):(float)val;
        }
        public string get_str(){
            return val.ToString();
        }
        public T get<T>(){
            switch(type){
                case TSDataType.NONE :
                    return (T)(object)null;
                default:
                    return (T)val;
            }
        }
        
        public byte[] get_bytes(){
            ByteBuffer buffer = new ByteBuffer(new byte[]{});
            buffer.add_int((int)type);
            switch(type){
                case TSDataType.BOOLEAN:
                    buffer.add_bool((bool)val);
                    break;
                case TSDataType.INT32:
                    buffer.add_int((Int32)val);
                    break;
                case TSDataType.INT64:
                    buffer.add_long((Int64)val);
                    break;
                case TSDataType.FLOAT:
                    buffer.add_float((float)val);
                    break;
                case TSDataType.DOUBLE:
                    buffer.add_double((double)val);
                    break;
                case TSDataType.TEXT:
                    buffer.add_str((string)val);
                    break;
                case TSDataType.NONE:
                    var err_msg = string.Format("NONE type does not support get bytes");
                    throw new TException(err_msg, null);
            }
            return buffer.get_buffer();
        }

        public override string ToString()
        {
            switch(type){
                case TSDataType.NONE:
                    return "NULL";
                default:
                    return val.ToString();
            }
        }

    }
}