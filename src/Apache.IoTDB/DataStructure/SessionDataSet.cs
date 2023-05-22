using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    public class SessionDataSet : System.IDisposable
    {
        private readonly long _queryId;
        private readonly long _statementId;
        private readonly string _sql;
        private readonly List<string> _columnNames;
        private readonly Dictionary<string, int> _columnNameIndexMap;
        private readonly Dictionary<int, int> _duplicateLocation;
        private readonly List<string> _columnTypeLst;
        private TSQueryDataSet _queryDataset;
        private readonly byte[] _currentBitmap;
        private readonly int _columnSize;
        private List<ByteBuffer> _valueBufferLst, _bitmapBufferLst;
        private ByteBuffer _timeBuffer;
        private readonly ConcurrentClientQueue _clientQueue;
        private int _rowIndex;
        private bool _hasCatchedResult;
        private RowRecord _cachedRowRecord;
        private bool _isClosed = false;
        private bool disposedValue;

        private string TimestampStr => "Time";
        private int StartIndex => 2;
        private int Flag => 0x80;
        private int DefaultTimeout => 10000;
        public int FetchSize { get; set; }
        public int RowCount { get; set; }
        public SessionDataSet(string sql, TSExecuteStatementResp resp, ConcurrentClientQueue clientQueue, long statementId)
        {
            _clientQueue = clientQueue;
            _sql = sql;
            _queryDataset = resp.QueryDataSet;
            _queryId = resp.QueryId;
            _statementId = statementId;
            _columnSize = resp.Columns.Count;
            _currentBitmap = new byte[_columnSize];
            _columnNames = new List<string>();
            _timeBuffer = new ByteBuffer(_queryDataset.Time);
            _columnNameIndexMap = new Dictionary<string, int>();
            _columnTypeLst = new List<string>();
            _duplicateLocation = new Dictionary<int, int>();
            _valueBufferLst = new List<ByteBuffer>();
            _bitmapBufferLst = new List<ByteBuffer>();
            // some internal variable
            _hasCatchedResult = false;
            _rowIndex = 0;
            RowCount = _queryDataset.Time.Length / sizeof(long);
            if (resp.ColumnNameIndexMap != null)
            {
                for (var index = 0; index < resp.Columns.Count; index++)
                {
                    _columnNames.Add("");
                    _columnTypeLst.Add("");
                }

                for (var index = 0; index < resp.Columns.Count; index++)
                {
                    var name = resp.Columns[index];
                    _columnNames[resp.ColumnNameIndexMap[name]] = name;
                    _columnTypeLst[resp.ColumnNameIndexMap[name]] = resp.DataTypeList[index];
                }
            }
            else
            {
                _columnNames = resp.Columns;
                _columnTypeLst = resp.DataTypeList;
            }

            for (int index = 0; index < _columnNames.Count; index++)
            {
                var columnName = _columnNames[index];
                if (_columnNameIndexMap.ContainsKey(columnName))
                {
                    _duplicateLocation[index] = _columnNameIndexMap[columnName];
                }
                else
                {
                    _columnNameIndexMap[columnName] = index;
                }

                _valueBufferLst.Add(new ByteBuffer(_queryDataset.ValueList[index]));
                _bitmapBufferLst.Add(new ByteBuffer(_queryDataset.BitmapList[index]));
            }

        }
        public List<string> ColumnNames => _columnNames;


        private List<string> GetColumnNames()
        {
            var lst = new List<string>
            {
                "timestamp"
            };
            lst.AddRange(_columnNames);
            return lst;
        }

        public void ShowTableNames()
        {
            var str = GetColumnNames()
                .Aggregate("", (current, name) => current + (name + "\t\t"));

            Console.WriteLine(str);
        }

        public bool HasNext()
        {
            if (_hasCatchedResult)
            {
                return true;
            }

            // we have consumed all current data, fetch some more
            if (!_timeBuffer.HasRemaining())
            {
                if (!FetchResults())
                {
                    return false;
                }
            }

            ConstructOneRow();
            _hasCatchedResult = true;
            return true;
        }

        public RowRecord Next()
        {
            if (!_hasCatchedResult)
            {
                if (!HasNext())
                {
                    return null;
                }
            }

            _hasCatchedResult = false;
            return _cachedRowRecord;
        }
        public RowRecord GetRow()
        {
            return _cachedRowRecord;
        }

        private TSDataType GetDataTypeFromStr(string str)
        {
            return str switch
            {
                "BOOLEAN" => TSDataType.BOOLEAN,
                "INT32" => TSDataType.INT32,
                "INT64" => TSDataType.INT64,
                "FLOAT" => TSDataType.FLOAT,
                "DOUBLE" => TSDataType.DOUBLE,
                "TEXT" => TSDataType.TEXT,
                "NULLTYPE" => TSDataType.NONE,
                _ => TSDataType.TEXT
            };
        }

        private void ConstructOneRow()
        {
            List<object> fieldLst = new List<Object>();

            for (int i = 0; i < _columnSize; i++)
            {
                if (_duplicateLocation.ContainsKey(i))
                {
                    var field = fieldLst[_duplicateLocation[i]];
                    fieldLst.Add(field);
                }
                else
                {
                    var columnValueBuffer = _valueBufferLst[i];
                    var columnBitmapBuffer = _bitmapBufferLst[i];

                    if (_rowIndex % 8 == 0)
                    {
                        _currentBitmap[i] = columnBitmapBuffer.GetByte();
                    }

                    object localField;
                    if (!IsNull(i, _rowIndex))
                    {
                        var columnDataType = GetDataTypeFromStr(_columnTypeLst[i]);


                        switch (columnDataType)
                        {
                            case TSDataType.BOOLEAN:
                                localField = columnValueBuffer.GetBool();
                                break;
                            case TSDataType.INT32:
                                localField = columnValueBuffer.GetInt();
                                break;
                            case TSDataType.INT64:
                                localField = columnValueBuffer.GetLong();
                                break;
                            case TSDataType.FLOAT:
                                localField = columnValueBuffer.GetFloat();
                                break;
                            case TSDataType.DOUBLE:
                                localField = columnValueBuffer.GetDouble();
                                break;
                            case TSDataType.TEXT:
                                localField = columnValueBuffer.GetStr();
                                break;
                            default:
                                string err_msg = "value format not supported";
                                throw new TException(err_msg, null);
                        }

                        fieldLst.Add(localField);
                    }
                    else
                    {
                        localField = null;
                        fieldLst.Add(DBNull.Value);
                    }
                }
            }

            long timestamp = _timeBuffer.GetLong();
            _rowIndex += 1;
            _cachedRowRecord = new RowRecord(timestamp, fieldLst, _columnNames);
        }

        private bool IsNull(int loc, int row_index)
        {
            byte bitmap = _currentBitmap[loc];
            int shift = row_index % 8;
            return ((Flag >> shift) & bitmap) == 0;
        }

        private bool FetchResults()
        {
            _rowIndex = 0;
            var myClient = _clientQueue.Take();
            var req = new TSFetchResultsReq(myClient.SessionId, _sql, FetchSize, _queryId, true)
            {
                Timeout = DefaultTimeout
            };
            try
            {
                var task = myClient.ServiceClient.fetchResultsAsync(req);

                var resp = task.ConfigureAwait(false).GetAwaiter().GetResult();

                if (resp.HasResultSet)
                {
                    _queryDataset = resp.QueryDataSet;
                    // reset buffer
                    _timeBuffer = new ByteBuffer(resp.QueryDataSet.Time);
                    _valueBufferLst = new List<ByteBuffer>();
                    _bitmapBufferLst = new List<ByteBuffer>();
                    for (int index = 0; index < _queryDataset.ValueList.Count; index++)
                    {
                        _valueBufferLst.Add(new ByteBuffer(_queryDataset.ValueList[index]));
                        _bitmapBufferLst.Add(new ByteBuffer(_queryDataset.BitmapList[index]));
                    }

                    // reset row index
                    _rowIndex = 0;
                }

                return resp.HasResultSet;
            }
            catch (TException e)
            {
                throw new TException("Cannot fetch result from server, because of network connection", e);
            }
            finally
            {
                _clientQueue.Add(myClient);
            }
        }

        public async Task Close()
        {
            if (!_isClosed)
            {
                var myClient = _clientQueue.Take();
                var req = new TSCloseOperationReq(myClient.SessionId)
                {
                    QueryId = _queryId,
                    StatementId = _statementId
                };

                try
                {
                    var status = await myClient.ServiceClient.closeOperationAsync(req);
                }
                catch (TException e)
                {
                    throw new TException("Operation Handle Close Failed", e);
                }
                finally
                {
                    _clientQueue.Add(myClient);
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    try
                    {
                        this.Close().Wait();
                    }
                    catch
                    {
                    }
                }
                _queryDataset = null;
                _timeBuffer = null;
                _valueBufferLst = null;
                _bitmapBufferLst = null;
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}