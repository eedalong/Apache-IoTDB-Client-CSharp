using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Threading;

namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Provides a simple way to create and manage the contents of connection strings used by
    ///     <see cref="IoTDBConnection" />.
    /// </summary>
    public class IoTDBConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const string DataSourceKeyword = "DataSource";
        private const string UserNameKeyword = "Username";
        private const string PasswordKeyword = "Password";
        private const string PortKeyword = "Port";
        private const string FetchSizeKeyword = "FetchSize";
        private const string CompressionKeyword = "Compression";
        private const string PoolSizeKeyword = "PoolSize";
        private const string ZoneIdKeyword = "ZoneId";
        private const string TimeOutKeyword = "TimeOut";

        private enum Keywords
        {
            DataSource,
            Username,
            Password,
            Port,
            FetchSize,
            Compression,
            PoolSize,
            ZoneId,
            TimeOut
        }

        private static readonly IReadOnlyList<string> _validKeywords;
        private static readonly IReadOnlyDictionary<string, Keywords> _keywords;

        private string _dataSource = "127.0.0.1";
        private string _userName = "root";
        private string _password = "root";
        private bool _enableRpcCompression = false;
        private int  _fetchSize = 1800;
        private string _zoneId = "UTC+08:00";
        private int  _port = 6667;
      private   int  _poolSize =8;
        private int _timeOut=10000;

        static IoTDBConnectionStringBuilder()
        {
            var validKeywords = new string[9];
            validKeywords[(int)Keywords.DataSource] = DataSourceKeyword;
             validKeywords[(int)Keywords.Username] = UserNameKeyword;
            validKeywords[(int)Keywords.Password] = PasswordKeyword;
            validKeywords[(int)Keywords.Port] = PortKeyword;
            validKeywords[(int)Keywords.FetchSize] = FetchSizeKeyword;
            validKeywords[(int)Keywords.Compression] = CompressionKeyword;
            validKeywords[(int)Keywords.PoolSize] = PoolSizeKeyword;
            validKeywords[(int)Keywords.ZoneId] = ZoneIdKeyword;
            validKeywords[(int)Keywords.TimeOut] =TimeOutKeyword;
            _validKeywords = validKeywords;

            _keywords = new Dictionary<string, Keywords>(9, StringComparer.OrdinalIgnoreCase)
            {
                [DataSourceKeyword] = Keywords.DataSource,
                [UserNameKeyword] = Keywords.Username,
                [PasswordKeyword] = Keywords.Password,
                [PortKeyword] = Keywords.Port,
                [FetchSizeKeyword] = Keywords.FetchSize,
                [CompressionKeyword] = Keywords.Compression,
                [PoolSizeKeyword] = Keywords.PoolSize,
                [ZoneIdKeyword] = Keywords.ZoneId,
                [TimeOutKeyword] = Keywords.TimeOut
            };
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBConnectionStringBuilder" /> class.
        /// </summary>
        public IoTDBConnectionStringBuilder()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="IoTDBConnectionStringBuilder" /> class.
        /// </summary>
        /// <param name="connectionString">
        ///     The initial connection string the builder will represent. Can be null.
        /// </param>
        public IoTDBConnectionStringBuilder(string connectionString)
            => ConnectionString = connectionString;

        /// <summary>
        ///     Gets or sets the database file.
        /// </summary>
        /// <value>The database file.</value>
        public virtual string DataSource
        {
            get => _dataSource;
            set => base[DataSourceKeyword] = _dataSource = value;
        }
        public virtual string Username
        {
            get => _userName;
            set => base[UserNameKeyword] = _userName = value;
        }
  
        public virtual string Password
        {
            get => _password;
            set => base[PasswordKeyword] = _password = value;
        }
        public virtual int  Port
        {
            get => _port;
            set => base[PortKeyword] = _port = value;
        }

        public virtual int FetchSize
        {
            get => _fetchSize;
            set => base[FetchSizeKeyword] = _fetchSize = value;
        }
        public virtual bool Compression
        {
            get => _enableRpcCompression;
            set => base[CompressionKeyword] = _enableRpcCompression = value;
        }
        public virtual int PoolSize
        {
            get => _poolSize;
            set => base[PoolSizeKeyword] = _poolSize = value;
        }
        public virtual string ZoneId
        {
            get => _zoneId;
            set => base[ZoneIdKeyword] = _zoneId = value;
        }

        public virtual int TimeOut
    {
            get => _timeOut;
            set => base[PoolSizeKeyword] = _timeOut = value;
        }

        /// <summary>
        ///     Gets a collection containing the keys used by the connection string.
        /// </summary>
        /// <value>A collection containing the keys used by the connection string.</value>
        public override ICollection Keys
            => new ReadOnlyCollection<string>((string[])_validKeywords);

        /// <summary>
        ///     Gets a collection containing the values used by the connection string.
        /// </summary>
        /// <value>A collection containing the values used by the connection string.</value>
        public override ICollection Values
        {
            get
            {
                var values = new object[_validKeywords.Count];
                for (var i = 0; i < _validKeywords.Count; i++)
                {
                    values[i] = GetAt((Keywords)i);
                }

                return new ReadOnlyCollection<object>(values);
            }
        }







        /// <summary>
        ///     Gets or sets the value associated with the specified key.
        /// </summary>
        /// <param name="keyword">The key.</param>
        /// <returns>The value.</returns>
        public override object this[string keyword]
        {
            get => GetAt(GetIndex(keyword));
            set
            {
                if (value == null)
                {
                    Remove(keyword);

                    return;
                }
           
                switch (GetIndex(keyword))
                {
                    case Keywords.DataSource:
                        DataSource = Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.Username:
                        Username= Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.Password:
                        Password = Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
      
                    case Keywords.Port:
                        Port = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.FetchSize:
                        FetchSize = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.Compression:
                        Compression = Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.PoolSize:
                        PoolSize = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.ZoneId:
                        ZoneId = Convert.ToString(value, CultureInfo.InvariantCulture);
                        return;
                    case Keywords.TimeOut:
                        TimeOut = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                        return;
                    default:
                        Debug.WriteLine(false, "Unexpected keyword: " + keyword);
                        return;
                }
            }
        }

        private static TEnum ConvertToEnum<TEnum>(object value)
            where TEnum : struct
        {
            if (value is string stringValue)
            {
                return (TEnum)Enum.Parse(typeof(TEnum), stringValue, ignoreCase: true);
            }

            if (value is TEnum enumValue)
            {
                enumValue = (TEnum)value;
            }
            else if (value.GetType().GetTypeInfo().IsEnum)
            {
                throw new ArgumentException($"ConvertFailed{value.GetType()},{typeof(TEnum)}");
            }
            else
            {
                enumValue = (TEnum)Enum.ToObject(typeof(TEnum), value);
            }

            if (!Enum.IsDefined(typeof(TEnum), enumValue))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    $"Invalid Enum Value{typeof(TEnum)},{enumValue}");
            }

            return enumValue;
        }

        /// <summary>
        ///     Clears the contents of the builder.
        /// </summary>
        public override void Clear()
        {
            base.Clear();

            for (var i = 0; i < _validKeywords.Count; i++)
            {
                Reset((Keywords)i);
            }
        }

        /// <summary>
        ///     Determines whether the specified key is used by the connection string.
        /// </summary>
        /// <param name="keyword">The key to look for.</param>
        /// <returns>true if it is use; otherwise, false.</returns>
        public override bool ContainsKey(string keyword)
            => _keywords.ContainsKey(keyword);

        /// <summary>
        ///     Removes the specified key and its value from the connection string.
        /// </summary>
        /// <param name="keyword">The key to remove.</param>
        /// <returns>true if the key was used; otherwise, false.</returns>
        public override bool Remove(string keyword)
        {
            if (!_keywords.TryGetValue(keyword, out var index)
                || !base.Remove(_validKeywords[(int)index]))
            {
                return false;
            }

            Reset(index);

            return true;
        }

        /// <summary>
        ///     Determines whether the specified key should be serialized into the connection string.
        /// </summary>
        /// <param name="keyword">The key to check.</param>
        /// <returns>true if it should be serialized; otherwise, false.</returns>
        public override bool ShouldSerialize(string keyword)
            => _keywords.TryGetValue(keyword, out var index) && base.ShouldSerialize(_validKeywords[(int)index]);

        /// <summary>
        ///     Gets the value of the specified key if it is used.
        /// </summary>
        /// <param name="keyword">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>true if the key was used; otherwise, false.</returns>
        public override bool TryGetValue(string keyword, out object value)
        {
            if (!_keywords.TryGetValue(keyword, out var index))
            {
                value = null;

                return false;
            }

            value = GetAt(index);

            return true;
        }

        private object GetAt(Keywords index)
        {
  
            switch (index)
            {
                case Keywords.DataSource:
                    return DataSource;
                case Keywords.Password:
                    return Password;
                case Keywords.Username:
                    return Username;
    
                case Keywords.Port:
                    return Port;
                case Keywords.FetchSize:
                    return FetchSize;
                case Keywords.Compression:
                    return Compression;
                case Keywords.PoolSize:
                    return PoolSize;
                case Keywords.ZoneId:
                    return ZoneId;
                case Keywords.TimeOut:
                    return TimeOut;
                default:
                    Debug.Assert(false, "Unexpected keyword: " + index);
                    return null;
            }
        }

        private static Keywords GetIndex(string keyword)
            => !_keywords.TryGetValue(keyword, out var index)
                ? throw new ArgumentException($"Keyword Not Supported{keyword}")
                : index;

        private void Reset(Keywords index)
        {
            switch (index)
            {
                case Keywords.DataSource:
                    _dataSource = "127.0.0.1";
                    return;
                case Keywords.Password:
                    _password = "root";
                    return;
                case Keywords.Username:
                    _userName = "root";
                    return;
                case Keywords.Port:
                    _port=6667;
                    return;
                case Keywords.FetchSize:
                    _fetchSize = 1800;
                    return;
                case Keywords.Compression:
                    _enableRpcCompression = false;
                    return;
                case Keywords.PoolSize:
                    _poolSize = 8;
                    return;
                case Keywords.ZoneId:
                    _zoneId = "UTC+08:00";
                    return;
                case Keywords.TimeOut:
                    _timeOut = 10000;//10sec.
                    return;
                default:
                    Debug.Assert(false, "Unexpected keyword: " + index);
                    return;
            }
        }
    }
}
