using System.Collections.Generic;
using iotdb_client_csharp.client.utils;
using System;


namespace iotdb_client_csharp.client.test
{
    public class UnitTest
    {
        public string host = "81.69.18.71";
        public int port = 8888;
        public string user = "root";
        public string passwd = "root";
        public int fetch_size = 5000;
        public bool debug = false;

        public UnitTest(){}
        public void Test(){
            TestField();
            TestRowRecord();
            TestSessionDataSet();
        }
        public void TestField(){
            double double_val_set = 12.3, double_val_get;
            int int_val_get;
            string  str_val_set = "12", str_val_get;
            bool bool_val_get, bool_val_set;
            float float_val_get;
            Int64 long_val_get;


            // set double
            Field field = new Field(TSDataType.DOUBLE);
            field.set(double_val_set);
            double_val_get = (double)field.get();
            System.Diagnostics.Debug.Assert(double_val_get == double_val_set);
            int_val_get = field.get_int();
            System.Diagnostics.Debug.Assert(int_val_get == Convert.ToInt32(double_val_set));
            str_val_get = field.get_str();
            System.Diagnostics.Debug.Assert(str_val_get == double_val_set.ToString());
            bool_val_get = field.get_bool();
            System.Diagnostics.Debug.Assert(bool_val_get == Convert.ToBoolean(double_val_set));
            float_val_get = field.get_float();
            System.Diagnostics.Debug.Assert(float_val_get ==  Convert.ToSingle(double_val_set));
            long_val_get = field.get_long();
            System.Diagnostics.Debug.Assert(long_val_get == Convert.ToInt64(double_val_set));

            // Set Str value
            field = new Field(TSDataType.TEXT);
            field.set(str_val_set);
            double_val_get = field.get_double();
            System.Diagnostics.Debug.Assert(double_val_get == double.Parse(str_val_set));
            int_val_get = field.get_int();
            System.Diagnostics.Debug.Assert(int_val_get == Int32.Parse(str_val_set));
            bool_val_get = field.get_bool();
            System.Diagnostics.Debug.Assert(bool_val_get);
            float_val_get = field.get_float();
            System.Diagnostics.Debug.Assert(float_val_get == float.Parse(str_val_set));
            long_val_get = field.get_long();
            System.Diagnostics.Debug.Assert(long_val_get == Int64.Parse(str_val_set));

            // set true
            field = new Field(TSDataType.BOOLEAN);
            bool_val_set = true;
            field.set(bool_val_set);
            double_val_get = field.get_double();
            System.Diagnostics.Debug.Assert(double_val_get == Convert.ToDouble(bool_val_set));
            
            System.Console.WriteLine("Field Test Passed!");
        }
        public void TestRowRecord(){
            var save_datetime = DateTime.Now.ToLocalTime();
            TimeSpan ts = save_datetime - DateTime.UnixEpoch.ToLocalTime();
            var row_reord = new RowRecord(Convert.ToInt64(ts.TotalMilliseconds), new List<Field>{});

            // test append
            row_reord.append(new Field(TSDataType.DOUBLE, 12.3));
            row_reord.append(new Field(TSDataType.BOOLEAN, false));
            row_reord.append(new Field(TSDataType.FLOAT, 12.3));
            row_reord.append(new Field(TSDataType.INT64, 1234));
            row_reord.append(new Field(TSDataType.TEXT, "TEST"));
            
            // test indexing 
            var field_get = row_reord[0];
            System.Diagnostics.Debug.Assert(field_get.type == TSDataType.DOUBLE);
            System.Diagnostics.Debug.Assert((double)field_get.val == 12.3);

            // test indexing 
            row_reord[0] = new Field(TSDataType.BOOLEAN, true);
            field_get = row_reord[0];
            System.Diagnostics.Debug.Assert(field_get.type == TSDataType.BOOLEAN);
            System.Diagnostics.Debug.Assert((bool)field_get.val == true);

            // test datetime
            var datetime = row_reord.get_date_time();
            Console.WriteLine(datetime.ToString() == save_datetime.ToString());
            System.Console.WriteLine("RowRecord Test Passed!");
        }

        public void TestSessionDataSet(){
            
        }
        


    }
}