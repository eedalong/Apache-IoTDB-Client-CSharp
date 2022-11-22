
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
        public static SessionPool CreateSession(this IoTDBConnectionStringBuilder db)
        {
            return new SessionPool(db.DataSource, db.Port, db.Username, db.Password, db.FetchSize, db.ZoneId, db.PoolSize,db.Compression,db.TimeOut);
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
                                var pr = from p in pots where (p.Name == strKey || p.ColumnNameIs(strKey)) && p.CanWrite select p;
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
        {
            var dt = new DataTable();
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
    }
}
