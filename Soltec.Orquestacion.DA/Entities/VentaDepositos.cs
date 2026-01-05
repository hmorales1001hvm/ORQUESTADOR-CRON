using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class VentaDepositos
	{
        public string SC_CVE { get; set; }
        public string SC_NOMBRE { get; set; }
        public string RFC { get; set; }
        public DateTime Fecha { get; set; }
        public decimal VENTA_NETA { get; set; }
    }
}
