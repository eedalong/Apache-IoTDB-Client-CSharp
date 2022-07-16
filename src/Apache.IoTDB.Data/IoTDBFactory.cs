using System.Data.Common;

namespace Apache.IoTDB.Data
{
    /// <summary>
    ///     Creates instances of various Maikebing.Data.IoTDB classes.
    /// </summary>
    public class IoTDBFactory : DbProviderFactory
    {
        private IoTDBFactory()
        {
        }

        /// <summary>
        ///     The singleton instance.
        /// </summary>
        public static readonly IoTDBFactory Instance = new IoTDBFactory();

        /// <summary>
        ///     Creates a new command.
        /// </summary>
        /// <returns>The new command.</returns>
        public override DbCommand CreateCommand()
            => new IoTDBCommand();

        /// <summary>
        ///     Creates a new connection.
        /// </summary>
        /// <returns>The new connection.</returns>
        public override DbConnection CreateConnection()
            => new IoTDBConnection();

        /// <summary>
        ///     Creates a new connection string builder.
        /// </summary>
        /// <returns>The new connection string builder.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
            => new IoTDBConnectionStringBuilder();

        /// <summary>
        ///     Creates a new parameter.
        /// </summary>
        /// <returns>The new parameter.</returns>
        public override DbParameter CreateParameter()
            => new IoTDBParameter();
    }
}
