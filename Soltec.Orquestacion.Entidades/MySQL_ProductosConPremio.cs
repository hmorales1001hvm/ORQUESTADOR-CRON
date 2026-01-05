using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.Entidades
{
    public class MySQL_ProductosConPremio
    {
        public DateTime Fecha { get; set; }
        public string IdVendedor { get; set; }
        public string Nombre { get; set; }
        public decimal ImporteVenta { get; set; }
        public int Transaccionesventa { get; set; }
        public decimal PorcVenta { get; set; }
        public decimal ImporteNaturistas { get; set; }
        public decimal PorcNaturistas { get; set; }
        public decimal ImporteNocturno { get; set; }
        public decimal MontoDescuento { get; set; }
        public decimal Menudeos { get; set; }
        public decimal MontoIva { get; set; }
    }
    public class ProductosConPremioWrapper
    {
        public List<MySQL_ProductosConPremio> ProductosConPremio { get; set; }
    }
}
