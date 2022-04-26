using System;
using System.Collections.Generic;
using System.Linq;
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    /*
    * A tablet data of one device, the tablet contains multiple measurements of this device that share
    * the same time column.
    *
    * for example:  device root.sg1.d1
    *
    * time, m1, m2, m3
    *    1,  1,  2,  3
    *    2,  1,  2,  3
    *    3,  1,  2,  3
    *
    * Notice: The tablet should not have empty cell
    *
    */
    public class Tablet
    {
        private readonly List<long> _timestamps;
        private readonly List<List<object>> _values;

        public string DeviceId { get; }
        public List<string> Measurements { get; }
        public List<TSDataType> DataTypes { get; }
        public BitMap[] BitMaps;
        public int RowNumber { get; }
        public int ColNumber { get; }

        private readonly Utils _utilFunctions = new Utils();

        public Tablet(
            string deviceId,
            List<string> measurements,
            List<TSDataType> dataTypes,
            List<List<object>> values,
            List<long> timestamps)
        {
            if (timestamps.Count != values.Count)
            {
                throw new TException(
                    $"Input error. Timestamps.Count({timestamps.Count}) does not equal to Values.Count({values.Count}).",
                    null);
            }

            if (measurements.Count != dataTypes.Count)
            {
                throw new TException(
                    $"Input error. Measurements.Count({measurements.Count}) does not equal to DataTypes.Count({dataTypes.Count}).",
                    null);
            }

            if (!_utilFunctions.IsSorted(timestamps))
            {
                var sorted = timestamps
                    .Select((x, index) => (timestamp: x, values: values[index]))
                    .OrderBy(x => x.timestamp).ToList();

                _timestamps = sorted.Select(x => x.timestamp).ToList();
                _values = sorted.Select(x => x.values).ToList();
            }
            else
            {
                _values = values;
                _timestamps = timestamps;
            }

            DeviceId = deviceId;
            Measurements = measurements;
            DataTypes = dataTypes;
            RowNumber = timestamps.Count;
            ColNumber = measurements.Count;

            // reset bitmap
            if (BitMaps != null)
            {
                foreach (var bitmap in BitMaps)
                {
                    if (bitmap != null)
                    {
                        bitmap.reset();
                    }
                }
            }
        }

        public byte[] GetBinaryTimestamps()
        {
            var buffer = new ByteBuffer(new byte[] { });

            foreach (var timestamp in _timestamps)
            {
                buffer.AddLong(timestamp);
            }

            return buffer.GetBuffer();
        }

        public List<int> GetDataTypes()
        {
            var dataTypeValues = DataTypes.ConvertAll(x => (int)x);

            return dataTypeValues;
        }

        private int EstimateBufferSize()
        {
            var estimateSize = 0;

            // estimate one row size
            foreach (var value in _values[0])
            {
                switch (value)
                {
                    case bool _:
                        estimateSize += 1;
                        break;
                    case int _:
                        estimateSize += 4;
                        break;
                    case long _:
                        estimateSize += 8;
                        break;
                    case float _:
                        estimateSize += 4;
                        break;
                    case double _:
                        estimateSize += 8;
                        break;
                    case string s:
                        estimateSize += s.Length;
                        break;
                }
            }

            estimateSize *= _timestamps.Count;
            return estimateSize;
        }

        public byte[] GetBinaryValues()
        {
            var estimateSize = EstimateBufferSize();
            var buffer = new ByteBuffer(estimateSize);

            for (var i = 0; i < ColNumber; i++)
            {
                var dataType = DataTypes[i];
                var values = _values[i];

                // set bitmap
                for (var j = 0; j < RowNumber; j++)
                {
                    var value = values[j];
                    if (value == null)
                    {
                        if (BitMaps == null)
                        {
                            BitMaps = new BitMap[ColNumber];
                        }
                        if (BitMaps[i] == null)
                        {
                            BitMaps[i] = new BitMap(RowNumber);
                        }
                        BitMaps[i].mark(j);
                    }
                }

                switch (dataType)
                {
                    case TSDataType.BOOLEAN:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddBool(value != null ? (bool)value : false);
                            }

                            break;
                        }
                    case TSDataType.INT32:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddInt(value != null ? (int)value : int.MinValue);
                            }

                            break;
                        }
                    case TSDataType.INT64:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddLong(value != null ? (long)value : long.MinValue);
                            }

                            break;
                        }
                    case TSDataType.FLOAT:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddFloat(value != null ? (float)value : float.MinValue);
                            }

                            break;
                        }
                    case TSDataType.DOUBLE:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddDouble(value != null ? (double)value : double.MinValue);
                            }

                            break;
                        }
                    case TSDataType.TEXT:
                        {
                            foreach (var value in values)
                            {
                                buffer.AddStr(value != null ? (string)value : string.Empty);
                            }

                            break;
                        }
                    default:
                        throw new TException($"Unsupported data type {dataType}", null);

                }
            }
            if (BitMaps != null)
            {
                foreach (var bitmap in BitMaps)
                {
                    bool columnHasNull = bitmap != null && !bitmap.isAllUnmarked();
                    buffer.AddBool((bool)columnHasNull);
                    if (columnHasNull)
                    {
                        var bytes = bitmap.getByteArray();
                        for (int i = 0; i < RowNumber / 8 + 1; i++)
                        {
                            buffer.AddByte(bytes[i]);
                        }
                    }
                }
            }

            return buffer.GetBuffer();
        }
        public void InitBitMaps()
        {
            BitMaps = new BitMap[ColNumber];
            for (var i = 0; i < ColNumber; i++)
            {
                BitMaps[i] = new BitMap(RowNumber);
            }
        }
    }
}