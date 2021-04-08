using System;
using System.Linq;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class ByteBuffer
    {
        private byte[] buffer;
        private List<byte> write_buffer;
        private int pos;
        private int total_length;
        public ByteBuffer(byte[] buffer){
            this.buffer = buffer;
            this.pos = 0;
            this.total_length = buffer.Length;
            this.write_buffer = new List<byte>{};
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
            int int_value = BitConverter.ToInt32(buffer, pos);
            pos += 4;
            return int_value;
        }
        public long get_long(){
            long long_value = BitConverter.ToInt64(buffer, pos);
            pos += 8;
            return long_value;
        }
        public float get_float(){
            float float_value = BitConverter.ToSingle(buffer, pos);
            pos += 4;
            return float_value;
        }
        public double get_double(){
            double double_value = BitConverter.ToDouble(buffer, pos);
            pos += 8;
            return double_value;
        }
        public string get_str(){
            int length = BitConverter.ToInt32(buffer, pos);
            pos += 1;
            string str_value = System.Text.Encoding.UTF8.GetString(buffer, pos, length);
            pos += length;
            return str_value;
        }
        public byte[] get_buffer(){
            return buffer;
        }
        // these for write
        public void add_bool(bool value){
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_int(Int32 value){
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_long(long value){
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_float(float value){
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_double(double value){
            write_buffer.AddRange(BitConverter.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length =  buffer.Length;
        }
        public void add_str(string value){
            write_buffer.AddRange(BitConverter.GetBytes(value.Length));
            write_buffer.AddRange(System.Text.Encoding.UTF8.GetBytes(value));
            buffer = write_buffer.ToArray();
            total_length = buffer.Length;
        }
        public void add_char(char value){
           write_buffer.AddRange(BitConverter.GetBytes(value));
           buffer = write_buffer.ToArray();
           total_length =  buffer.Length; 
        }

    }
    
}