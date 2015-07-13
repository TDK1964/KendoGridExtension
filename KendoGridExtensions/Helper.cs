using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace KendoGridExtensions
{
    public class Helper
    {
        public static IDbConnection GetConnection()
        {
            var _connectionString = "";
            Type _connectionType = null;

            var connectionSettings = ConfigurationManager.ConnectionStrings["DefaultConnection"];

            if (connectionSettings == null)
                throw new ArgumentNullException("Cannot find connection settings");

            if (connectionSettings.ProviderName == "System.Data.SqlClient")
                _connectionType = typeof(SqlConnection);
            else
                _connectionType = Type.GetType(connectionSettings.ProviderName);

            _connectionString = connectionSettings.ConnectionString;
            
            var connection = (IDbConnection)Activator.CreateInstance(_connectionType, _connectionString);
            connection.Open();
            return connection;
        }

  private bool IsValidDateTime(DateTime dateTime)
        {
            var isValid = false;

            if (dateTime == null) return true;

            var testDate = DateTime.MinValue;
            var minValue = DateTime.Parse(System.Data.SqlTypes.SqlDateTime.MinValue.ToString());
            var maxValue = DateTime.Parse(System.Data.SqlTypes.SqlDateTime.MaxValue.ToString());

            System.Data.SqlTypes.SqlDateTime sdt;

            if (minValue > dateTime || maxValue < dateTime)
                return false;

            if (DateTime.TryParse(dateTime.ToString(), out testDate))
            {
                try
                {
                    // take advantage of the native conversion
                    sdt = new System.Data.SqlTypes.SqlDateTime(testDate);
                    isValid = true;
                }
                catch (System.Data.SqlTypes.SqlTypeException ex)
                {

                    // no need to do anything, this is the expected out of range error
                }
            }

            return true;
        }


    }
}