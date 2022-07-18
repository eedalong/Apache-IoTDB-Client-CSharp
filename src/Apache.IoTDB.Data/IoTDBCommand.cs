

using Apache.IoTDB.DataStructure;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
 

namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Represents a SQL statement to be executed against a IoTDB database.
    /// </summary>
    public class IoTDBCommand : DbCommand
    {
        private readonly Lazy<IoTDBParameterCollection> _parameters = new Lazy<IoTDBParameterCollection>(
            () => new IoTDBParameterCollection());
        private IoTDBConnection _connection;
        private string _commandText;
        private SessionPool _IoTDB => _connection._IoTDB;
        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBCommand" /> class.
        /// </summary>
        public IoTDBCommand()
        {

        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBCommand" /> class.
        /// </summary>
        /// <param name="commandText">The SQL to execute against the database.</param>
        public IoTDBCommand(string commandText)
            => CommandText = commandText;

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBCommand" /> class.
        /// </summary>
        /// <param name="commandText">The SQL to execute against the database.</param>
        /// <param name="connection">The connection used by the command.</param>
        public IoTDBCommand(string commandText, IoTDBConnection connection)
            : this(commandText)
        {
            Connection = connection;
            CommandTimeout = connection.DefaultTimeout;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBCommand" /> class.
        /// </summary>
        /// <param name="commandText">The SQL to execute against the database.</param>
        /// <param name="connection">The connection used by the command.</param>
        /// <param name="transaction">The transaction within which the command executes.</param>
        public IoTDBCommand(string commandText, IoTDBConnection connection, IoTDBTransaction transaction)
            : this(commandText, connection)
            => Transaction = transaction;

        /// <summary>
        ///     Gets or sets a value indicating how <see cref="CommandText" /> is interpreted. Only
        ///     <see cref="CommandType.Text" /> is supported.
        /// </summary>
        /// <value>A value indicating how <see cref="CommandText" /> is interpreted.</value>
        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentException($"Invalid CommandType{value}");
                }
            }
        }

        /// <summary>
        ///     Gets or sets the SQL to execute against the database.
        /// </summary>
        /// <value>The SQL to execute against the database.</value>
        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException($"SetRequiresNoOpenReader{nameof(CommandText)}");
                }

                if (value != _commandText)
                {
                    _commandText = value;
                }
            }
        }

        /// <summary>
        ///     Gets or sets the connection used by the command.
        /// </summary>
        /// <value>The connection used by the command.</value>
        public new virtual IoTDBConnection Connection
        {
            get => _connection;
            set
            {
                if (DataReader != null)
                {
                    throw new InvalidOperationException($"SetRequiresNoOpenReader{nameof(Connection)}");
                }

                if (value != _connection)
                {

                    _connection?.RemoveCommand(this);
                    _connection = value;
                    value?.AddCommand(this);
                }
            }
        }

        /// <summary>
        ///     Gets or sets the connection used by the command. Must be a <see cref="IoTDBConnection" />.
        /// </summary>
        /// <value>The connection used by the command.</value>
        protected override DbConnection DbConnection
        {
            get => Connection;
            set => Connection = (IoTDBConnection)value;
        }

        /// <summary>
        ///     Gets or sets the transaction within which the command executes.
        /// </summary>
        /// <value>The transaction within which the command executes.</value>
        public new virtual IoTDBTransaction Transaction { get; set; }

        /// <summary>
        ///     Gets or sets the transaction within which the command executes. Must be a <see cref="IoTDBTransaction" />.
        /// </summary>
        /// <value>The transaction within which the command executes.</value>
        protected override DbTransaction DbTransaction
        {
            get => Transaction;
            set => Transaction = (IoTDBTransaction)value;
        }

        /// <summary>
        ///     Gets the collection of parameters used by the command.
        /// </summary>
        /// <value>The collection of parameters used by the command.</value>
        public new virtual IoTDBParameterCollection Parameters
            => _parameters.Value;

        /// <summary>
        ///     Gets the collection of parameters used by the command.
        /// </summary>
        /// <value>The collection of parameters used by the command.</value>
        protected override DbParameterCollection DbParameterCollection
            => Parameters;

        /// <summary>
        ///     Gets or sets the number of seconds to wait before terminating the attempt to execute the command. Defaults to 30.
        /// </summary>
        /// <value>The number of seconds to wait before terminating the attempt to execute the command.</value>
        /// <remarks>
        ///     The timeout is used when the command is waiting to obtain a lock on the table.
        /// </remarks>
        public override int CommandTimeout { get; set; } = 30;

        /// <summary>
        ///     Gets or sets a value indicating whether the command should be visible in an interface control.
        /// </summary>
        /// <value>A value indicating whether the command should be visible in an interface control.</value>
        public override bool DesignTimeVisible { get; set; }

        /// <summary>
        ///     Gets or sets a value indicating how the results are applied to the row being updated.
        /// </summary>
        /// <value>A value indicating how the results are applied to the row being updated.</value>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        ///     Gets or sets the data reader currently being used by the command, or null if none.
        /// </summary>
        /// <value>The data reader currently being used by the command.</value>
        protected internal virtual IoTDBDataReader DataReader { get; set; }

        /// <summary>
        ///     Releases any resources used by the connection and closes it.
        /// </summary>
        /// <param name="disposing">
        ///     true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {

            base.Dispose(disposing);
        }

        /// <summary>
        ///     Creates a new parameter.
        /// </summary>
        /// <returns>The new parameter.</returns>
        public new virtual IoTDBParameter CreateParameter()
            => new IoTDBParameter();

        /// <summary>
        ///     Creates a new parameter.
        /// </summary>
        /// <returns>The new parameter.</returns>
        protected override DbParameter CreateDbParameter()
            => CreateParameter();

        /// <summary>
        ///     Creates a prepared version of the command on the database.
        /// </summary>
        public override void Prepare()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(Prepare)}");
            }

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(Prepare)}");
            }

        }

        /// <summary>
        ///     Executes the <see cref="CommandText" /> against the database and returns a data reader.
        /// </summary>
        /// <returns>The data reader.</returns>
        /// <exception cref="IoTDBException">A IoTDB error occurs during execution.</exception>
        public new virtual IoTDBDataReader ExecuteReader()
            => ExecuteReader(CommandBehavior.Default);


  

        /// <summary>
        ///     Executes the <see cref="CommandText" /> against the database and returns a data reader.
        /// </summary>
        /// <param name="behavior">
        ///     A description of the results of the query and its effect on the database.
        ///     <para>
        ///         Only <see cref="CommandBehavior.Default" />, <see cref="CommandBehavior.SequentialAccess" />,
        ///         <see cref="CommandBehavior.SingleResult" />, <see cref="CommandBehavior.SingleRow" />, and
        ///         <see cref="CommandBehavior.CloseConnection" /> are supported.
        ///     </para>
        /// </param>
        /// <returns>The data reader.</returns>
        /// <exception cref="IoTDBException">A IoTDB error occurs during execution.</exception>
        public new virtual IoTDBDataReader ExecuteReader(CommandBehavior behavior)
        {
            if ((behavior & ~(CommandBehavior.Default | CommandBehavior.SequentialAccess | CommandBehavior.SingleResult
                              | CommandBehavior.SingleRow | CommandBehavior.CloseConnection)) != 0)
            {
                throw new ArgumentException($"InvalidCommandBehavior{behavior}");
            }

            if (DataReader != null)
            {
                throw new InvalidOperationException($"DataReaderOpen");
            }

            if (_connection?.State != ConnectionState.Open)
            {
                _connection.Open();
                if (_connection?.State != ConnectionState.Open)
                {
                    throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteReader)}");
                }
            }
         

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteReader)}");
            }

            var unprepared = false;
            IoTDBDataReader dataReader = null;
            var closeConnection = (behavior & CommandBehavior.CloseConnection) != 0;
            try
            {
#if DEBUG
                Debug.WriteLine($"_commandText:{_commandText}");
#endif
                int _affectRows = 0;
                SessionDataSet dataSet=null;
                bool isok = false;
                Task<SessionDataSet> taskDataSet = null;
                if (_parameters.IsValueCreated)
                {

                    var pms = _parameters.Value;
                    var record = BindParamters(pms);
                    throw new NotSupportedException();
                }
                else
                {
                    
                    var sessionData = Task.Run(() => _IoTDB.ExecuteQueryStatementAsync(_commandText));
                    isok = sessionData.Wait(TimeSpan.FromSeconds(CommandTimeout));
                    if (isok)
                    {
                        dataSet = sessionData.Result;
                        _affectRows = dataSet.FetchSize;
                    }
                }

                if (isok && dataSet != null  )
                {
                    dataReader = new IoTDBDataReader(this, dataSet, closeConnection  );
                }
                else if (taskDataSet.Status == TaskStatus.Running || !isok)
                {
                    IoTDBException.ThrowExceptionForRC(-10006, "Execute sql command timeout", null);
                }
                else if (taskDataSet.IsCanceled)
                {
                    IoTDBException.ThrowExceptionForRC(-10003, "Command is Canceled", null);
                }
                else if (taskDataSet.IsFaulted)
                {
                    IoTDBException.ThrowExceptionForRC(-10004, taskDataSet.Exception.Message, taskDataSet.Exception?.InnerException);
                }
                else
                {
                    IoTDBException.ThrowExceptionForRC(_commandText, new IoTDBErrorResult() { Code = -10007, Error = $"Unknow Exception" });
                }
            }
            catch when (unprepared)
            {
                throw;
            }
            return dataReader;
        }

        private RowRecord BindParamters(IoTDBParameterCollection pms)
        {
            var measures = new List<string>();
            var values = new List<object> ();
           

            for (int i = 0; i < pms.Count; i++)
            {

                var tp = pms[i];
                measures.Add(tp.ParameterName);
             //   _commandText = _commandText.Replace(tp.ParameterName, "?");
                switch (TypeInfo.GetTypeCode(tp.Value?.GetType()))
                {
                    case TypeCode.Boolean:
                       values.Add ((tp.Value as bool?).GetValueOrDefault());
                        break;
                    case TypeCode.Char:
                        values.Add(tp.Value as string);
                        break;
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                        values.Add((tp.Value as byte?).GetValueOrDefault());
                        break;
                    case TypeCode.DateTime:
                        var t0 = tp.Value as DateTime?;
                        if (!t0.HasValue)
                        {
                            throw new ArgumentException($"InvalidArgumentOfDateTime{tp.Value}");
                        }
                        values.Add(t0.GetValueOrDefault());
                        break;
                    case TypeCode.DBNull:
                   
                        break;
                    case TypeCode.Single:
                        values.Add((tp.Value as float?).GetValueOrDefault());
                        break;
                    case TypeCode.Decimal:
                    case TypeCode.Double:
                        values.Add((tp.Value as double?).GetValueOrDefault());
                        break;
                    case TypeCode.Int16:
                        values.Add((tp.Value as short?).GetValueOrDefault());
                        break;
                    case TypeCode.Int32:
                        values.Add((tp.Value as int?).GetValueOrDefault());
                        break;
                    case TypeCode.Int64:
                        values.Add((tp.Value as long?).GetValueOrDefault());
                        break;
                    case TypeCode.UInt16:
                        values.Add((tp.Value as short?).GetValueOrDefault());
                        break;
                    case TypeCode.UInt32:
                        values.Add((tp.Value as uint?).GetValueOrDefault());
                        break;
                    case TypeCode.UInt64:
                        values.Add((tp.Value as ulong?).GetValueOrDefault());
                        break;
                    case TypeCode.String:
                    default:
                        values.Add(tp.Value as string);
                        break;
                }
            }

            return   new RowRecord(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),values,measures);
        }

        /// <summary>
        ///     Executes the <see cref="CommandText" /> against the database and returns a data reader.
        /// </summary>
        /// <param name="behavior">A description of query's results and its effect on the database.</param>
        /// <returns>The data reader.</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => ExecuteReader(behavior);

        /// <summary>
        ///     Executes the <see cref="CommandText" /> asynchronously against the database and returns a data reader.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        ///     IoTDB does not support asynchronous execution. Use write-ahead logging instead.
        /// </remarks>
        /// <seealso href="http://IoTDB.org/wal.html">Write-Ahead Logging</seealso>
        public new virtual Task<IoTDBDataReader> ExecuteReaderAsync()
            => ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

        /// <summary>
        ///     Executes the <see cref="CommandText" /> asynchronously against the database and returns a data reader.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        ///     IoTDB does not support asynchronous execution. Use write-ahead logging instead.
        /// </remarks>
        /// <seealso href="http://IoTDB.org/wal.html">Write-Ahead Logging</seealso>
        public new virtual Task<IoTDBDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
            => ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

        /// <summary>
        ///     Executes the <see cref="CommandText" /> asynchronously against the database and returns a data reader.
        /// </summary>
        /// <param name="behavior">A description of query's results and its effect on the database.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        ///     IoTDB does not support asynchronous execution. Use write-ahead logging instead.
        /// </remarks>
        /// <seealso href="http://IoTDB.org/wal.html">Write-Ahead Logging</seealso>
        public new virtual Task<IoTDBDataReader> ExecuteReaderAsync(CommandBehavior behavior)
            => ExecuteReaderAsync(behavior, CancellationToken.None);

        /// <summary>
        ///     Executes the <see cref="CommandText" /> asynchronously against the database and returns a data reader.
        /// </summary>
        /// <param name="behavior">A description of query's results and its effect on the database.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        ///     IoTDB does not support asynchronous execution. Use write-ahead logging instead.
        /// </remarks>
        /// <seealso href="http://IoTDB.org/wal.html">Write-Ahead Logging</seealso>
        public new virtual Task<IoTDBDataReader> ExecuteReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(ExecuteReader(behavior));
        }

        /// <summary>
        ///     Executes the <see cref="CommandText" /> asynchronously against the database and returns a data reader.
        /// </summary>
        /// <param name="behavior">A description of query's results and its effect on the database.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
            => await ExecuteReaderAsync(behavior, cancellationToken);

        /// <summary>
        ///     Executes the <see cref="CommandText" /> against the database.
        /// </summary>
        /// <returns>The number of rows inserted, updated, or deleted. -1 for SELECT statements.</returns>
        /// <exception cref="IoTDBException">A IoTDB error occurs during execution.</exception>
        public override int ExecuteNonQuery()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteNonQuery)}");
            }
            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteNonQuery)}");
            }
            var result = Task.Run(() => _IoTDB.ExecuteNonQueryStatementAsync(_commandText));
             var ok = result.Wait(TimeSpan.FromSeconds(CommandTimeout));
            if (!ok) throw new  TimeoutException();
            return result.Result;
        }

        /// <summary>
        ///     Executes the <see cref="CommandText" /> against the database and returns the result.
        /// </summary>
        /// <returns>The first column of the first row of the results, or null if no results.</returns>
        /// <exception cref="IoTDBException">A IoTDB error occurs during execution.</exception>
        public override object ExecuteScalar()
        {
            if (_connection?.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(ExecuteScalar)}");
            }
            if (_commandText == null)
            {
                throw new InvalidOperationException($"CallRequiresSetCommandText{nameof(ExecuteScalar)}");
            }

            using (var reader = ExecuteReader())
            {
                return reader.Read()
                    ? reader.GetValue(0)
                    : null;
            }
        }
      
        /// <summary>
        ///     Attempts to cancel the execution of the command. Does nothing.
        /// </summary>
        public override void Cancel()
        {
        }

      
    }
}
