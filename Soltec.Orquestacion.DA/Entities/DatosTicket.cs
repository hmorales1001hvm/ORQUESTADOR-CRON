using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
	public class DatosTicket
	{
			public int idDatosTicket { get; set; }
			public string sucursal { get; set; }
			public string codigoBarras { get; set; }
			public decimal total { get; set; }
			public string rfc { get; set; }
			public string razonSocial { get; set; }
			public string cp { get; set; }
			public string idRegimenFiscal { get; set; }
			public string claveCfdi { get; set; }
			public string formaPago { get; set; }
			public string correo { get; set; }
			public DateTime fechaCaptura { get; set; }
			public string uuid { get; set; }
			public string archivoxml { get; set; }
			public int Estatus { get; set; }
			public int empresa_id { get; set; }
			public decimal TotalTicket { get; set; }
			public decimal TotalFacturado { get; set; }
			public string NotaCredito { get; set; }
			public int NotaProcesada { get; set; }
			public string RFCEmisor { get; set; }
			public int Prueba { get; set; }
			public string uuidNota { get; set; }
			public int convertido { get; set; }
			public string NotaOK { get; set; }
			public int FacturaCancelada { get; set; }
			public int NotaCancelada { get; set; }
			public int simifactura { get; set; }
			public string TicketConsolidado { get; set; }
			public decimal IvaFactura { get; set; }
			public decimal IvaNotaCredito { get; set; }
			public decimal DescuentoFactura { get; set; }
			public decimal DescuentoNota { get; set; }
			public DateTime fechaNCR { get; set; }
			public string uuidrelacionado { get; set; }
			public string Comentarios { get; set; }
			public DateTime fechaTicket { get; set; }
			public string sucursalNCR { get; set; }
			public string vCfdi { get; set; }
			public string errorDescripcion { get; set; }
			public string pais { get; set; }
			public string registroTributario { get; set; }
			public string ErrorDescripcionNCR { get; set; }
			public int Multifran { get; set; }
			public string codigoError { get; set; }
			public int MultifranNCR { get; set; }
			public DateTime fechaCreacion { get; set; }
			public DateTime fechaModificacion { get; set; }
			public int ticketEnCentral { get; set; }
			public DateTime FechaTimbrado { get; set; }
			public string codigoErrorNCR { get; set; }
			public decimal TotalNCR { get; set; }
			public string UUIDFacturaGlobal { get; set; }
			public string sistemaTimbra { get; set; }
			public decimal ImpuestoTasa { get; set; }
			public decimal ImpuestoBase { get; set; }
			public string serieNCR { get; set; }
			public int AnioCaptura { get; set; }
			public int MesCaptura { get; set; }
    }
}
