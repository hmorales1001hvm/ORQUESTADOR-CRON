using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.Entidades
{
    public class TransmisionHistorico
    {
        public string Nombre { get; set; }
        public int IdSucursal { get; set; }
        public bool ConTransmisionInicial { get; set; }
        public bool ConDatos { get; set; }
        public int MultiFra { get; set; }
        public string DatabaseName { get; set; }
        public string HostName { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string Sucursal { get; set; }

    }
}
