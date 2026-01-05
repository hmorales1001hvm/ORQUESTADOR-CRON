using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class Conciliacion
	{
        public long IdTransaction { get; set; }
		public DateTime FechaHora { get; set; }
		public string Tarjeta { get; set; }
		public decimal Monto { get; set; }
		public decimal Propina { get; set; }
		public decimal MontoTotal { get; set; }
		public decimal PorcentajeComision { get; set; }
		public decimal MontoComision { get; set; }
		public decimal IvaComision { get; set; }
		public decimal MSI { get; set; }
		public decimal SobreTasa { get; set; }
		public decimal MontoSobreTasa { get; set; }
		public decimal IvaSobreTasa { get; set; }
		public decimal FondoSeguridad { get; set; }
		public decimal FondoSeguridadAsignado { get; set; }
		public decimal FondoSeguridadRetenido { get; set; }
		public decimal MontoDepositar { get; set; }
		public string Autorizacion { get; set; }
		public string TipoTransaccion { get; set; }
		public string MarcaTarjeta { get; set; }
		public string TipoTarjeta { get; set; }
		public string TipoCaptura { get; set; }
		public string Banco { get; set; }
		public string Pais { get; set; }
		public string EtiquedaDispositivo { get; set; }
		public string ReferenciaTransaccion { get; set; }
		public string IdUsuario { get; set; }
		public string NombreComercial { get; set; }
		public string RazonSocial { get; set; }
		public string Afiliacion { get; set; }
		public string Lector { get; set; }
		public DateTime FechaHoraTransConsi { get; set; }
		public string Distribuidor { get; set; }
		public string Grupo { get; set; }
		public DateTime FechaDeposito { get; set; }
		public string Correo { get; set; }
		public string TipoPago { get; set; }
		public string Moneda { get; set; }
		public decimal ImporteComision { get; set; }
		public decimal ImporteDepositado { get; set; }

	}
}
