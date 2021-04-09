using System;
using System.Linq;
using System.Collections.Generic;
using System.Net;
namespace iotdb_client_csharp.client.utils
{
    public class ByteBuffer
    {
        private byte[] buffer;
        private List<byte> write_buffer;
        private int pos;
        private int total_length;
        private bool is_little_endian;
        public ByteBuffer(byte[] buffer){            
            this.buffer = buffer;
            this.pos = 0;
            this.total_length = buffer.Length;
            this.write_buffer = new List<byte>{};
            this.is_little_endian = BitConverter.IsLittleEndian;
        }
        public bool has_remaining(){
            return pos < total_length;
        }
        // these for read
        public byte get_byte(){
            var byte_val = buffer[pos];
            pos += 1;
            return byte_val;
        }
        public bool get_bool(){
            bool bool_value = BitConverter.ToBoolean(buffer, pos);
            pos += 1;
            return bool_value;
        }
        public int get_int(){

            var int_buff = buffer[pos..(pos+4)];
            if(is_little_endian){
                int_buff = int_buff.Reverse().ToArray();
            }
            int int_value = BitConverter.ToInt32(int_buff);
            pos += 4;
            return int_value;
        }
        public long get_long(){
            var long_buff = buffer[pos..(pos + 8)];
            if(is_little_endian){
                long_buff = long_buff.Reverse().ToArray();
            }
            long long_value = BitConverter.ToInt64(long_buff);
            pos += 8;
            return long_value;
        }
        public float get_float(){
            var float_buff = buffer[pos..(pos +4)];
            if(is_little_endian){
                float_buff = float_buff.Reverse().ToArray();
            }
            float float_value = BitConverter.ToSingle(float_buff);
            pos += 4;
            return float_value;
        }
        public double get_double(){
            var double_buff = buffer[pos..(pos+8)];
            if(is_little_endian){
                double_buff = double_buff.Reverse().ToArray();
            }
            double double_value = BitConverter.ToDouble(double_buff);
            pos += 8;
            return double_value;
        }
        public string get_str(){
            int length = get_int();
            var str_buff = buffer[pos..(pos+length)];
            string str_value = System.Text.Encoding.UTF8.GetString(str_buff);
            pos += length;
            return str_value;
        }
        public byte[] get_buffer(){
            return buffer;
        }
        // these for write
        public void add_bool(bool value){
            var bool_buffer = BitConverter.GetBytes(value);
            if(is_little_endian){
                bool_buffer = bool_buffer.Reverse().ToArray();
            }
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_int(Int32 value){
            var int_buff = BitConverter.GetBytes(value); 
            if(is_little_endian){
                int_buff = int_buff.Reverse().ToArray();
            }
            write_buffer.AddRange(int_buff);
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_long(long value){
            var long_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                long_buff = long_buff.Reverse().ToArray();
            }
            write_buffer.AddRange(long_buff);
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_float(float value){
            var float_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                float_buff = float_buff.Reverse().ToArray();
            }
            write_buffer.AddRange(float_buff);
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_double(double value){
            var double_buff = BitConverter.GetBytes(value);
            if(is_little_endian){
                double_buff = double_buff.Reverse().ToArray();
            }
            write_buffer.AddRange(double_buff);
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_str(string value){
            add_int(value.Length);
            var str_buf = System.Text.Encoding.UTF8.GetBytes(value);
            if(is_little_endian){
                str_buf = str_buf.Reverse().ToArray();
            } 
            write_buffer.AddRange(str_buf);
            buffer = write_buffer.ToArray();
            total_length = buffer.Length;
        }
        public void add_char(char value){
           var char_buf = BitConverter.GetBytes(value);
           if(is_little_endian){
               char_buf = char_buf.Reverse().ToArray();
           }
           write_buffer.AddRange(char_buf);
           buffer = write_buffer.ToArray();
           total_length =  buffer.Length; 
        }

    }
    
}