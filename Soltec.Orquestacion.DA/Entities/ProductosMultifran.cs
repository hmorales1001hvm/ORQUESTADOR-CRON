using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.DA.Entities
{
    public class ProductosMultifran
    {
        public string NoIdentificacion { get; set; }
        public string Descripcion { get; set; }
        public decimal Compra { get; set; }
        public decimal Venta { get; set; }
        public bool EsInvent { get; set; }
        public bool EsKit { get; set; }
    }
}
