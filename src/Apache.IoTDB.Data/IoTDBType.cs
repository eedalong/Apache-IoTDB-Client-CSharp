 
namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Represents the type affinities used by columns in IoTDB tables.
    /// </summary>
  
    public enum IoTDBType
    {
        /// <summary>
        ///     A signed integer.
        /// </summary>
        Integer , 

        /// <summary>
        ///     A floating point value.
        /// </summary>
        Real  ,

        /// <summary>
        ///     A text string.
        /// </summary>
        Text ,

        /// <summary>
        ///     A blob of data.
        /// </summary>
        Blob 
    }
}
