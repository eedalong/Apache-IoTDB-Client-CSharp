using System;
using System.Collections.Generic;
namespace iotdb_client_csharp.client.utils
{
    public class Tablet
    {
        // TBD by Zengz
       private string device_id;
       private List<string> measurements;
       private List<TSDataType> data_types;
       private List<long> timestamps;
       private List<List<string>> values;

       public Tablet(string device_id, List<string> measurements, List<TSDataType> data_types, List<List<string>> values, List<long> timestamps){
           if(timestamps.Count != values.Count){
               var err_msg = String.Format("Input error! len(timestamps) does not equal to len(values)!");
               Console.WriteLine(err_msg);
               throw new Exception("input length not matched");
           }

           this.device_id = device_id;
           this.measurements = measurements;
           this.data_types = data_types;
           this.values = values;
           this.timestamps = timestamps;
       }




    }
}