using System.Collections.Generic;
using Thrift;
using System;
namespace iotdb_client_csharp.client.utils
{
    public class RowRecord
    {
        public long timestamp{get;set;}
        public List<Object> values {get;set;}
        public List<string> measurements{get;set;}
        public RowRecord(long timestamp, List<Object> values, List<string> measurements){
            this.timestamp = timestamp;
            this.values = values;
            this.measurements = measurements;
        }
        public void append(string measurement, Object value){
            values.Add(value);
            measurements.Add(measurement);
        }
        public DateTime get_date_time(){
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime.ToLocalTime();;
        }

        public override string ToString()
        {
            var str = "TimeStamp";
             foreach(var measurement in measurements){
                str += "\t\t";
                str += measurement.ToString();
            }
            str += "\n";
            
            str += timestamp.ToString();
            foreach(var row_value in values){
                str += "\t\t";
                str += row_value.ToString();
            }
            return str;
        }
        



    }
}