using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
 
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Represents a connection to a IoTDB database.
    /// </summary>
    public partial class IoTDBConnection : DbConnection
    {
 

        private readonly IList<WeakReference<IoTDBCommand>> _commands = new List<WeakReference<IoTDBCommand>>();

        private string _connectionString;
        private ConnectionState _state;
        internal SessionPool  _IoTDB;
      
 
     
        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBConnection" /> class.
        /// </summary>
        public IoTDBConnection()
        {
            _IoTDB = (ConnectionStringBuilder ?? new IoTDBConnectionStringBuilder()).CreateSession();
        }
        public SessionPool SessionPool => _IoTDB;

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBConnection" /> class.
        /// </summary>
        /// <param name="connectionString">The string used to open the connection.</param>
        /// <seealso cref="IoTDBConnectionStringBuilder" />
        public IoTDBConnection(string connectionString) : this()
        {
            ConnectionStringBuilder = new IoTDBConnectionStringBuilder(connectionString);
            ConnectionString = connectionString;
        }



        /// <summary>
        ///     Gets or sets a string used to open the connection.
        /// </summary>
        /// <value>A string used to open the connection.</value>
        /// <seealso cref="IoTDBConnectionStringBuilder" />
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                _connectionString = value;
                ConnectionStringBuilder = new IoTDBConnectionStringBuilder(value);
                _IoTDB = ConnectionStringBuilder.CreateSession();
            }
        }

        internal IoTDBConnectionStringBuilder ConnectionStringBuilder { get; set; }


        /// <summary>
        ///     Gets the path to the database file. Will be absolute for open connections.
        /// </summary>
        /// <value>The path to the database file.</value>
        public override string DataSource
        {
            get
            {
                string dataSource = null;

                return dataSource ?? ConnectionStringBuilder.DataSource;
            }
        }

        /// <summary>
        ///     Gets or sets the default <see cref="IoTDBCommand.CommandTimeout"/> value for commands created using
        ///     this connection. This is also used for internal commands in methods like
        ///     <see cref="BeginTransaction()"/>.
        /// </summary>
        /// <value>The default <see cref="IoTDBCommand.CommandTimeout"/> value</value>
        public virtual int DefaultTimeout { get; set; } = 60;


   
        /// <summary>
        ///     Gets the version of IoTDB used by the connection.
        /// </summary>
        /// <value>The version of IoTDB used by the connection.</value>
        public override string ServerVersion
        {
            get
            {
                throw new NotImplementedException();
            }
        }
        public   string ClientVersion
        {
            get
            {

                throw new NotImplementedException();
            }
        }
        /// <summary>
        ///     Gets the current state of the connection.
        /// </summary>
        /// <value>The current state of the connection.</value>
        public override ConnectionState State
            => _state;

        /// <summary>
        ///     Gets the <see cref="DbProviderFactory" /> for this connection.
        /// </summary>
        /// <value>The <see cref="DbProviderFactory" />.</value>
        protected override DbProviderFactory DbProviderFactory
            => IoTDBFactory.Instance;

        /// <summary>
        ///     Gets or sets the transaction currently being used by the connection, or null if none.
        /// </summary>
        /// <value>The transaction currently being used by the connection.</value>
        protected internal virtual IoTDBTransaction Transaction { get; set; }

 




        private void SetState(ConnectionState value)
        {
            var originalState = _state;
            if (originalState != value)
            {
                _state = value;
                OnStateChange(new StateChangeEventArgs(originalState, value));
            }
        }

        /// <summary>
        ///     Opens a connection to the database using the value of <see cref="ConnectionString" />. If
        ///     <c>Mode=ReadWriteCreate</c> is used (the default) the file is created, if it doesn't already exist.
        /// </summary>
        /// <exception cref="IoTDBException">A IoTDB error occurs while opening the connection.</exception>
        public override void Open()
        {
            OpenAsync().GetAwaiter().GetResult();
        }
        public   override   async Task OpenAsync(CancellationToken cancellationToken=default)
        {
         
            if (State == ConnectionState.Open)
            {
                return;
            }
            if (ConnectionString == null)
            {
                throw new InvalidOperationException("Open Requires Set ConnectionString");
            }

            await _IoTDB.Open(ConnectionStringBuilder.Compression, cancellationToken);
           if (!_IoTDB.IsOpen())
            {
                IoTDBException.ThrowExceptionForRC(-1, "Can't open IoTDB server.");
            }
            else
            {
                SetState(ConnectionState.Open);
            }
        }

        /// <summary>
        ///     Closes the connection to the database. Open transactions are rolled back.
        /// </summary>
        public override void Close()
        {
            CloseAsync().GetAwaiter().GetResult(); ;
        }
#if NET461_OR_GREATER || NETSTANDARD2_0
        public  async Task CloseAsync()
#else
        public override async Task CloseAsync()
#endif
        { 
            if (State != ConnectionState.Closed)
                await  _IoTDB.Close();
            Transaction?.Dispose();
            _nowdatabase = string.Empty;
            foreach (var reference in _commands)
            {
                if (reference.TryGetTarget(out var command))
                {
                    command.Dispose();
                }
            }
            _commands.Clear();
            SetState(ConnectionState.Closed);
        }

        /// <summary>
        ///     Releases any resources used by the connection and closes it.
        /// </summary>
        /// <param name="disposing">
        ///     true to release managed and unmanaged resources; false to release only unmanaged resources.
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            base.Dispose(disposing);
        }

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        /// <remarks>
        ///     The command's <seealso cref="IoTDBCommand.Transaction" /> property will also be set to the current
        ///     transaction.
        /// </remarks>
        public new virtual IoTDBCommand CreateCommand()
            => new IoTDBCommand { Connection = this, CommandTimeout = DefaultTimeout, Transaction = Transaction };
        public virtual IoTDBCommand CreateCommand(string commandtext)
          => new IoTDBCommand { Connection = this, CommandText = commandtext, CommandTimeout = DefaultTimeout, Transaction = Transaction };

        /// <summary>
        ///     Creates a new command associated with the connection.
        /// </summary>
        /// <returns>The new command.</returns>
        protected override DbCommand CreateDbCommand()
            => CreateCommand();

        internal void AddCommand(IoTDBCommand command)
            => _commands.Add(new WeakReference<IoTDBCommand>(command));

        internal void RemoveCommand(IoTDBCommand command)
        {
            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                if (!_commands[i].TryGetTarget(out var item)
                    || item == command)
                {
                    _commands.RemoveAt(i);
                }
            }
        }

        /// <summary>
        ///     Create custom collation.
        /// </summary>
        /// <param name="name">Name of the collation.</param>
        /// <param name="comparison">Method that compares two strings.</param>
        public virtual void CreateCollation(string name, Comparison<string> comparison)
            => CreateCollation(name, null, comparison != null ? (_, s1, s2) => comparison(s1, s2) : (Func<object, string, string, int>)null);

        /// <summary>
        ///     Create custom collation.
        /// </summary>
        /// <typeparam name="T">The type of the state object.</typeparam>
        /// <param name="name">Name of the collation.</param>
        /// <param name="state">State object passed to each invocation of the collation.</param>
        /// <param name="comparison">Method that compares two strings, using additional state.</param>
        public virtual void CreateCollation<T>(string name, T state, Func<T, string, string, int> comparison)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(CreateCollation)}");
            }


        }

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <returns>The transaction.</returns>
        public new virtual IoTDBTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => BeginTransaction(isolationLevel);

        /// <summary>
        ///     Begins a transaction on the connection.
        /// </summary>
        /// <param name="isolationLevel">The isolation level of the transaction.</param>
        /// <returns>The transaction.</returns>
        public new virtual IoTDBTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"CallRequiresOpenConnection{nameof(BeginTransaction)}");
            }
            if (Transaction != null)
            {
                throw new InvalidOperationException($"ParallelTransactionsNotSupported");
            }

            return Transaction = new IoTDBTransaction(this, isolationLevel);
        }
        internal string _nowdatabase = string.Empty;

        internal bool SelectedDataBase => _nowdatabase != string.Empty ;

        public override string Database => throw new  NotSupportedException();

        /// <summary>
        ///     Changes the current database.  
        /// </summary>
        /// <param name="databaseName">The name of the database to use.</param>
        /// <exception cref="PlatformNotSupportedException"></exception>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public bool DatabaseExists(string databaseName)
        {
            throw new NotSupportedException();
        }
        
     
    }
}
