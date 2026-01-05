using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.Entidades
{
    public class MySQL_Ventas
    {
        public DateTime fechaOperacion { get; set; }
        public decimal Total { get; set; }
        public int tickets { get; set; }
    }
    public class VentasWrapper
    {
        public List<MySQL_Ventas> Ventas { get; set; }
    }
}
