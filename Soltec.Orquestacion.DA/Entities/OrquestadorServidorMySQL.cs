using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class OrquestadorServidorMySQL
	{
        public int IdEmpresa { get; set; }
        public string Empresa { get; set; }
		public string HostName { get; set; }
        public string HostNamePublic { get; set; }
        public string ClaveSimi { get; set; }
        public string UserName { get; set; }
		public string Password { get; set; }
		public string DatabaseName { get; set; }
		public string Sucursal { get; set; }
        public string DBReference { get; set; }
		public string UserNameReference { get; set; }
		public string PasswordReference { get; set; }
		public string Port { get; set; }
        public string Puerto { get; set; }
        public int IdSucursal { get; set; }
        public string UrlSQS { get; set; } = string.Empty;

        public string ConnectionString =>
                   $"Server={HostNamePublic};" +
                   $"Database={DatabaseName};" +
                   $"User Id={UserName};" +
                   $"Password={Password};";

    }
}
