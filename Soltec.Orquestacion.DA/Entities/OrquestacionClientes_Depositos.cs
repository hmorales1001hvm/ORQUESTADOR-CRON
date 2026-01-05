using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{ 
	public class OrquestacionClientes_Depositos
	{
		public int IdEmpresa { get; set; }
        public string Dominio { get; set; }
        public string Usuario { get; set; }
        public string Password { get; set; }
        public string DB { get; set; }
        public string FechaInicial { get; set; }
        public string FechaFinal { get; set; }
    }
}
