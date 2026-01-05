using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.Entidades
{
    public class InventarioSQS
    {
        public string FechaOperacion { get; set; }
        public string Id_Producto { get; set; }
        public int ExistenciaInicial { get; set; }
        public int Entradas { get; set; }
        public int Salidas { get; set; }
        public int ExistenciaFinal { get; set; }
    }
}
