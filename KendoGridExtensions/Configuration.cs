using System.Configuration;

namespace KendoGridExtensions
{
    public class Configuration
    {
        public static string ConnectionString
        {
            get { return ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString; }
        }

        public static string CrudAction(string action)
        {
            return ConfigurationManager.AppSettings[action];
        }


    }
}