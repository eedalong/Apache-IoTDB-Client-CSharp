using System;
using System.Collections.Generic;
using System.Linq;
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    public class RowRecord
    {
        public long Timestamps { get; }
        public List<object> Values { get; }
        public List<string> Measurements { get; }

        public RowRecord(DateTime timestamp, List<object> values, List<string> measurements)
            :this(new DateTimeOffset(timestamp.ToUniversalTime()).ToUnixTimeMilliseconds(), values,measurements)
        {
        }
        public RowRecord(long timestamps, List<object> values, List<string> measurements)
        {
            Timestamps = timestamps;
            Values = values;
            Measurements = measurements;
        }

        public void Append(string measurement, object value)
        {
            Values.Add(value);
            Measurements.Add(measurement);
        }

        public DateTime GetDateTime()
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(Timestamps).DateTime.ToLocalTime();
        }

        public override string ToString()
        {
            var str = "TimeStamp";
            foreach (var measurement in Measurements)
            {
                str += "\t\t";
                str += measurement;
            }

            str += "\n";

            str += Timestamps.ToString();
            foreach (var rowValue in Values)
            {
                str += "\t\t";
                str += rowValue.ToString();
            }

            return str;
        }

        public List<int> GetDataTypes()
        {
            var dataTypeValues = new List<int>();
            
            foreach (var valueType in Values.Select(value => value))
            {
                switch (valueType)
                {
                    case bool _:
                        dataTypeValues.Add((int) TSDataType.BOOLEAN);
                        break;
                    case int _:
                        dataTypeValues.Add((int) TSDataType.INT32);
                        break;
                    case long _:
                        dataTypeValues.Add((int) TSDataType.INT64);
                        break;
                    case float _:
                        dataTypeValues.Add((int) TSDataType.FLOAT);
                        break;
                    case double _:
                        dataTypeValues.Add((int) TSDataType.DOUBLE);
                        break;
                    case string _:
                        dataTypeValues.Add((int) TSDataType.TEXT);
                        break;
                }
            }

            return dataTypeValues;
        }
        public TypeCode GetTypeCode(int index)
        {
            TypeCode tSDataType = TypeCode.Empty;
            var valueType = Values[index];
            switch (valueType)
            {
                case bool _:
                    tSDataType = TypeCode.Boolean;
                    break;
                case int _:
                    tSDataType = TypeCode.Int32;
                    break;
                case long _:
                    tSDataType = TypeCode.Int64;
                    break;
                case float _:
                    tSDataType = TypeCode.Single;
                    break;
                case double _:
                    tSDataType = TypeCode.Double;
                    break;
                case string _:
                    tSDataType = TypeCode.String;
                    break;
            }
            return tSDataType;
        }
        public Type GetCrlType(int index)
        {
            Type tSDataType =  typeof(object);
            var valueType = Values[index];
            switch (valueType)
            {
                case bool _:
                    tSDataType = typeof( bool);
                    break;
                case int _:
                    tSDataType = typeof(int);
                    break;
                case long _:
                    tSDataType = typeof(long);
                    break;
                case float _:
                    tSDataType = typeof(float);
                    break;
                case double _:
                    tSDataType = typeof(double);
                    break;
                case string _:
                    tSDataType = typeof(string);
                    break;
            }
            return tSDataType;
        }

        public TSDataType GetDataType(int index)
        {
            TSDataType tSDataType = TSDataType.NONE;
            var valueType = Values[index];
            switch (valueType)
            {
                case bool _:
                    tSDataType = TSDataType.BOOLEAN;
                    break;
                case int _:
                    tSDataType = TSDataType.INT32;
                    break;
                case long _:
                    tSDataType = TSDataType.INT64;
                    break;
                case float _:
                    tSDataType = TSDataType.FLOAT;
                    break;
                case double _:
                    tSDataType = TSDataType.DOUBLE;
                    break;
                case string _:
                    tSDataType = TSDataType.TEXT;
                    break;
            }
            return tSDataType;
        }


        public byte[] ToBytes()
        {
            var buffer = new ByteBuffer(Values.Count * 8);
            
            foreach (var value in Values)
            {
                switch (value)
                {
                    case bool b:
                        buffer.AddByte((byte) TSDataType.BOOLEAN);
                        buffer.AddBool(b);
                        break;
                    case int i:
                        buffer.AddByte((byte) TSDataType.INT32);
                        buffer.AddInt(i);
                        break;
                    case long l:
                        buffer.AddByte((byte) TSDataType.INT64);
                        buffer.AddLong(l);
                        break;
                    case double d:
                        buffer.AddByte((byte) TSDataType.DOUBLE);
                        buffer.AddDouble(d);
                        break;
                    case float f:
                        buffer.AddByte((byte) TSDataType.FLOAT);
                        buffer.AddFloat(f);
                        break;
                    case string s:
                        buffer.AddByte((byte) TSDataType.TEXT);
                        buffer.AddStr(s);
                        break;
                    default:
                        throw new TException($"Unsupported data type:{value.GetType()}", null);
                }
            }

            return buffer.GetBuffer();;
        }
    }
}