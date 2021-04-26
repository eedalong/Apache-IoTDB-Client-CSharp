using System.Collections.Generic;
using Thrift;
using System;
namespace iotdb_client_csharp.client.utils
{
    public class RowRecord
    {
        public long timestamp{get;set;}
        public List<Object> row {get;set;}
        public RowRecord(long timestamp, List<Object> row){
            this.timestamp = timestamp;
            this.row = row;
        }
        public void append(Object value){
            row.Add(value);
        }

        public void set_filed(int index, Object value){
            row[index] = value;
        }
        public Object this[int index]{
            get => row[index];
            set => row[index] = value;
        }
        public DateTime get_date_time(){
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime.ToLocalTime();;
        }
        public override string ToString()
        {
            var str = timestamp.ToString();
            foreach(var row_value in row){
                str += "\t\t";
                str += row_value.ToString();
            }
            return str;
        }
        



    }
}