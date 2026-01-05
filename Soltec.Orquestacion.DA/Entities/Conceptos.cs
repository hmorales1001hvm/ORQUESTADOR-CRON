using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class Conceptos
	{
        public string? ClaveProdServ { get; set; }
		public string? NoIdentificacion { get; set; }
		public decimal Cantidad { get; set; }
		public string? ClaveUnidad { get; set; }
		public string? Unidad { get; set; }
		public string? Descripcion { get; set; }
		public decimal ValorUnitario { get; set; }
		public decimal Importe { get; set; }
		public decimal Descuento { get; set; }
        public string? Folio { get; set; }
        public DateTime Fecha { get; set; }
        public string? UUID { get; set; }
        public string FileName { get; set; }
    }
}
