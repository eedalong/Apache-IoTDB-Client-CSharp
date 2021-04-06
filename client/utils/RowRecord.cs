using System.Collections.Generic;
using Thrift;
namespace iotdb_client_csharp.client.utils
{
    public class RowRecord
    {
        private long timestamp{get;set;}
        private List<Field> field_lst{get;set;}
        public RowRecord(long timestamp, List<Field> field_lst){
            this.timestamp = timestamp;
            this.field_lst = field_lst;
        }
        public void add_filed(Field field){
            field_lst.Add(field);
        }

        public void add_filed<T>(TSDataType data_type, T value){
            var filed = new Field(data_type);
            filed.set_value(value);
            field_lst.Add(filed);
        }

        public void set_filed(int index, Field filed){
            field_lst[index] = filed;
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