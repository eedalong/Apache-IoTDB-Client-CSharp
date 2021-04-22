using System.Collections.Generic;
using Thrift;
using System;
namespace iotdb_client_csharp.client.utils
{
    public class RowRecord
    {
        public long timestamp{get;set;}
        public List<Field> field_lst{get;set;}
        public RowRecord(long timestamp, List<Field> field_lst){
            this.timestamp = timestamp;
            this.field_lst = field_lst;
        }
        public void add_field(Field field){
            field_lst.Add(field);
        }
        public void append(Field field){
            field_lst.Add(field);
        }

        public void set_filed(int index, Field filed){
            field_lst[index] = filed;
        }
        public Field this[int index]{
            get => field_lst[index];
            set => field_lst[index] = value;
        }
        public DateTime get_date_time(){
            return DateTime.UnixEpoch.AddMilliseconds(timestamp);
        }
        public override string ToString()
        {
            var str = timestamp.ToString();
            foreach(var field in field_lst){
                str += "\t\t";
                str += field.ToString();
            }
            return str;
        }
        



    }
}