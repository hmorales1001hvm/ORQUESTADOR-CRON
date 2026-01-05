using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.Entidades
{
    public class ModelTransmisiones
    {
        public int Id { get; set; }
        public string? Empresa { get; set; }
        public string? Sucursal { get; set; }
        public string? Tipo { get; set; }
        public string? Clave { get; set; }
        public int Dias { get; set; }
        public string? Desde { get; set; }
        public string? Hasta { get; set; }
        public string? TransmitirDesde { get; set; }
        public string? TransmitirHasta { get; set; }
        public string? Vigencia { get; set; }
        public string? PeriodoTransmision { get; set; }
        public bool Activo { get; set; }
        public string? Activo2 { get; set; }

        public int IdEmpresa { get; set; }
        public int IdSucursal { get; set; }
        public int IdSQLScript { get; set; }
        public int IdUsuario { get; set; }
        public string? NombreUsuario { get; set; }

        public string? LlaveUnica { get; set; }
        public string? Estatus { get; set; }
        public string? FechaCreacion { get; set; }
        public string? FechaRecibido { get; set; }
        public string? FechaProcesado { get; set; }
        public int? TipoCarga { get; set; }

    }
}
