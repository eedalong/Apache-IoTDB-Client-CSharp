using System;
using System.Collections.Generic;
using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class Field
    {
        // TBD By Zengz
        public object val;
        public TSDataType type{get;set;}
        public Field(TSDataType data_type){
            this.type = data_type;
        }
        public Field(TSDataType data_type, object val){
            this.type = data_type;
            this.val = val;
        }
        public void set<T>(T value){
            val = value;
        }
        public double get_double(){
            switch(type){
                case TSDataType.TEXT:
                    return double.Parse((string)val);
                case TSDataType.BOOLEAN:
                    return (bool)val?1.0:0;
                case TSDataType.NONE:
                    return 0.0;
                default:
                    return Convert.ToDouble(val);
                
            }
        }
        public Int32 get_int(){
            switch(type){
                case TSDataType.TEXT:
                    return Int32.Parse((string)val);
                case TSDataType.BOOLEAN:
                    return (bool)val?1:0;
                case TSDataType.NONE:
                    return 0;
                default:
                    return Convert.ToInt32(val);
            }
        }
        public Int64 get_long(){
            switch(type){
                case TSDataType.TEXT:
                    return Int64.Parse((string)val);
                case TSDataType.BOOLEAN:
                    return (bool)val?1:0;
                case TSDataType.NONE:
                    return 0;
                default:
                    return Convert.ToInt64(val);
            }
        }

        public float get_float(){
            switch(type){
                case TSDataType.TEXT:
                    return float.Parse((string)val);
                case TSDataType.BOOLEAN:
                    return (bool)val?1:0;
                case TSDataType.NONE:
                    return 0;
                default:
                    return Convert.ToSingle(val);
            }
        }
        public bool get_bool(){
            switch(type){
                case TSDataType.TEXT:
                    try{
                        return Convert.ToBoolean((string)val);
                    }
                    catch(System.FormatException){
                        return ((string)val).Length > 0;
                    }
                case TSDataType.NONE:
                    return false;
                default:
                    return Convert.ToBoolean(val);
            }
            
        }
        public string get_str(){
            return val.ToString();
        }
        public object get(){
            switch(type){
                case TSDataType.NONE :
                    return null;
                default:
                    return val;
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