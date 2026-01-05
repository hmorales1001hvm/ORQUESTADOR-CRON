using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class SPOS_SQLScripts
	{
		public int IdSqlScript { get; set; }
		public string SQLScript { get; set; }
		public string Nombre { get; set; }
		public string Tipo { get; set; }
		public string Condicion { get; set; }
		public bool EsAPI { get; set; }
		public bool Activo { get; set; }
		public string Descripcion { get; set; }
        public bool EsCatalogo { get; set; }
        public bool MultiplesTablas { get; set; }
        public int TiempoTransmision { get; set; }


    }
}
