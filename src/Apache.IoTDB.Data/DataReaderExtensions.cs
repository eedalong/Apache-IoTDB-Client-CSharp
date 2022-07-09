 
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Apache.IoTDB.Data
{
    public static class DataReaderExtensions
    {
        public static SessionPool CreateSession(this IoTDBConnectionStringBuilder  db)
        {
            return new SessionPool(db.DataSource, db.Port, db.Username, db.Password, db.FetchSize, db.ZoneId, db.PoolSize);
        }
     
        public static List<T> ToObject<T>(this IDataReader dataReader)
        {
            List<T> jArray = new List<T>();
            try
            {
                var t = typeof(T);
                var pots = t.GetProperties();
                while (dataReader.Read())
                {
                    T jObject = Activator.CreateInstance<T>();
                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        try
                        {
                            string strKey = dataReader.GetName(i);
                            if (dataReader[i] != DBNull.Value)
                            {
                                var pr = from p in pots where (p.Name == strKey ||  p.ColumnNameIs(strKey)) && p.CanWrite select p;
                                if (pr.Any())
                                {
                                    var pi = pr.FirstOrDefault();
                                    pi.SetValue(jObject, Convert.ChangeType(dataReader[i], pi.PropertyType));
                                }
                            }
                        }
                        catch (Exception)
                        {

                        }
                    }
                    jArray.Add(jObject);
                }
            }
            catch (Exception ex)
            {
                IoTDBException.ThrowExceptionForRC(-10002, $"ToObject<{nameof(T)}>  Error", ex);
            }
            return jArray;
        }

        internal static bool ColumnNameIs(this System.Reflection.PropertyInfo p, string strKey)
        {
            return (p.IsDefined(typeof(ColumnAttribute), true) && (p.GetCustomAttributes(typeof(ColumnAttribute), true) as ColumnAttribute[])?.FirstOrDefault().Name == strKey);
        }

      
        public static DataTable ToDataTable(this IDataReader reader)
        { var dt = new DataTable();
            try
            {
               
                dt.Load(reader, LoadOption.OverwriteChanges, (object sender, FillErrorEventArgs e) =>
                {

                });
            }
            catch (Exception)
            {

        
            }
            return dt;
        }
        public static string RemoveNull(this string str)
        {
            return str?.Trim('\0');
        }

        public static IntPtr  ToIntPtr(this long val)
        {
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(long));
            Marshal.WriteInt64(lenPtr, val);
            return lenPtr;
        }
        public static IntPtr ToIntPtr(this int  val)
        {
            IntPtr lenPtr = Marshal.AllocHGlobal(sizeof(int ));
            Marshal.WriteInt32(lenPtr, val);
            return lenPtr;
        }
       internal  struct UTF8IntPtrStruct
        {
         public   IntPtr ptr;
     public       int len;
        }

        internal static UTF8IntPtrStruct ToUTF8IntPtr(this string command)
        {
            UTF8IntPtrStruct result;
#if NET5_0_OR_GREATER
            IntPtr commandBuffer = Marshal.StringToCoTaskMemUTF8(command);
            int bufferlen = Encoding.UTF8.GetByteCount(command);
#else
            var bytes = Encoding.UTF8.GetBytes(command);
            int bufferlen = bytes.Length;
            IntPtr commandBuffer = Marshal.AllocHGlobal(bufferlen);
            Marshal.Copy(bytes, 0, commandBuffer, bufferlen);
#endif
            result.ptr = commandBuffer;
            result.len = bufferlen;
            return result;
        }

        public static void FreeUtf8IntPtr(this IntPtr ptr)
        {
#if NET5_0_OR_GREATER
            Marshal.FreeCoTaskMem(ptr);
#else
            Marshal.FreeHGlobal(ptr);
#endif
        }


    }
}
