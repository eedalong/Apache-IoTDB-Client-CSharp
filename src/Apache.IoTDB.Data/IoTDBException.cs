using System;
using System.Data.Common;
 

namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Represents a IoTDB error.
    /// </summary>
    public class IoTDBException : DbException
    {
        IoTDBErrorResult _IoTDBError;

        public IoTDBException(IoTDBErrorResult IoTDBError) : base(IoTDBError.Error, null)
        {
            _IoTDBError = IoTDBError;
            base.HResult = _IoTDBError.Code;
        }

        public IoTDBException(IoTDBErrorResult IoTDBError, Exception ex) : base(IoTDBError.Error, ex)
        {
            _IoTDBError = IoTDBError;
            base.HResult = _IoTDBError.Code;
        }


   

      
        public override string Message => _IoTDBError?.Error;
        public override int ErrorCode =>   (int) _IoTDBError?.Code;
        /// <summary>
        ///     Throws an exception with a specific IoTDB error code value.
        /// </summary>
        /// <param name="rc">The IoTDB error code corresponding to the desired exception.</param>
        /// <param name="db">A handle to database connection.</param>
        /// <remarks>
        ///     No exception is thrown for non-error result codes.
        /// </remarks>
        public static void ThrowExceptionForRC(string _commandText, IoTDBErrorResult IoTDBError)
        {
            var te = new IoTDBException(IoTDBError);
            te.Data.Add("commandText", _commandText);
            throw te;
        }
        public static void ThrowExceptionForRC( IoTDBErrorResult IoTDBError)
        {
            var te = new IoTDBException(IoTDBError);
            throw te;
        }
        public static void ThrowExceptionForRC(IntPtr _IoTDB)
        {
            var te = new IoTDBException(new IoTDBErrorResult() {   });
            throw te;
        }
        public static void ThrowExceptionForRC(int code, string message, Exception ex)
        {
            var te = new IoTDBException(new IoTDBErrorResult() { Code = code, Error = message }, ex);
            throw te;
        }
        public static void ThrowExceptionForRC(int code, string message)
        {
            var te = new IoTDBException(new IoTDBErrorResult() { Code = code, Error = message });
            throw te;
        }
    }
}
