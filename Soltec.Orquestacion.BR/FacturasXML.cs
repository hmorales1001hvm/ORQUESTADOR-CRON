using ClosedXML.Excel;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Renci.SshNet;
//using Soltec.Common.Logger;
using Soltec.Orquestacion.DA.Entities;
using System.Data;
using System.Xml.Linq;

namespace Soltec.Orquestacion.BR
{
    public class FacturasXML
    {
        private readonly ILogger<OnDemand> _logger;
        private readonly ApiSettings _apiSettings;

        public FacturasXML(ILogger<OnDemand> logger, IOptions<ApiSettings> apiSettings)
        {
            _logger = logger;
            _apiSettings = apiSettings.Value;
        }

        public async Task<bool> ProcesaFacturasConceptosXML(string rutaFacturasXML, CancellationToken ct)
        {
            if (!Directory.Exists(rutaFacturasXML))
            {
                //Logger.Info("Ruta no encontrada para procesar los XMLs");
                return false;
            }

            //Logger.Info("Procesando los XMLs");

            int contadorArchivos = 0;
            string fileName = string.Empty;
            var conceptos = new List<Conceptos>();

            var files = new DirectoryInfo(rutaFacturasXML)
                .GetFiles("*.xml")
                .OrderBy(f => f.CreationTime)
                .ToList();

            foreach (var file in files)
            {
                ct.ThrowIfCancellationRequested();

                //Logger.Info($"Procesando XML: {file.FullName}");

                bool procesado = await ProcesaConceptos(
                    file.FullName,
                    file.Name,
                    conceptos,
                    ct);

                if (!procesado)
                    continue;

                var destino = file.FullName + ".pro";
                if (!File.Exists(destino))
                    File.Move(file.FullName, destino);

                fileName = file.Name;
                contadorArchivos++;

                if (contadorArchivos >= 10)
                {
                    await ProcesarLote(conceptos, fileName, ct);
                    conceptos.Clear();
                    contadorArchivos = 0;
                }
            }

            // Lote final
            if (conceptos.Any())
            {
                await ProcesarLote(conceptos, fileName, ct);
                conceptos.Clear();
            }

            return true;
        }


        private async Task ProcesarLote(List<Conceptos> conceptos, string fileName, CancellationToken ct)
        {
            if (!conceptos.Any())
                return;

            ct.ThrowIfCancellationRequested();

            //Logger.Info($"Preparando DataTable temporal: {conceptos.Count}");

            var jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(conceptos);
            DataTable dataTable = await PrepareCopyDataTable(jsonString);

            //Logger.Info($"Iniciando carga masiva con {conceptos.Count} conceptos...");
            await Soltec.Orquestacion.DA.Orchestration
                .BulkCopyTableMySQLAsync(dataTable, fileName);
        }

        public async Task<bool> ProcesaConceptos(string file, string fileName, List<Conceptos> conceptos, CancellationToken ct)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                XDocument xml = XDocument.Load(file);

                XNamespace cfdi = "http://www.sat.gob.mx/cfd/4";
                XNamespace tfd = "http://www.sat.gob.mx/TimbreFiscalDigital";

                var comprobante = xml.Element(cfdi + "Comprobante");
                if (comprobante == null)
                    return false;

                var tipoDeComprobante = comprobante.Attribute("TipoDeComprobante")?.Value;
                if (tipoDeComprobante != "I")
                    return false;

                var emisor = comprobante.Element(cfdi + "Emisor");
                var rfc = emisor?.Attribute("Rfc")?.Value;

                if (string.IsNullOrEmpty(rfc) || rfc.ToUpper() != "FSI970908ML5")
                    return false;

                var complemento = comprobante.Element(cfdi + "Complemento");
                var timbre = complemento?.Element(tfd + "TimbreFiscalDigital");
                var uuid = timbre?.Attribute("UUID")?.Value ?? string.Empty;

                var folio = comprobante.Attribute("Folio")?.Value;
                var fechaStr = comprobante.Attribute("Fecha")?.Value;

                if (!DateTime.TryParse(fechaStr, out var fecha))
                    return false;

                var conceptosXml = xml.Descendants(cfdi + "Concepto");

                foreach (var concepto in conceptosXml)
                {
                    ct.ThrowIfCancellationRequested();

                    //Logger.Info($"Se va a subir la factura: {folio} - {fecha}");

                    conceptos.Add(new Conceptos
                    {
                        Folio = folio,
                        Fecha = fecha,
                        ClaveProdServ = concepto.Attribute("ClaveProdServ")?.Value,
                        NoIdentificacion = concepto.Attribute("NoIdentificacion")?.Value,
                        ClaveUnidad = concepto.Attribute("ClaveUnidad")?.Value,
                        Unidad = concepto.Attribute("Unidad")?.Value,
                        Descripcion = concepto.Attribute("Descripcion")?.Value,
                        UUID = uuid,
                        FileName = fileName,
                        Cantidad = ParseDecimal(concepto.Attribute("Cantidad")?.Value),
                        ValorUnitario = ParseDecimal(concepto.Attribute("ValorUnitario")?.Value),
                        Importe = ParseDecimal(concepto.Attribute("Importe")?.Value),
                        Descuento = ParseDecimal(concepto.Attribute("Descuento")?.Value)
                    });
                }

                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                //Logger.Error($"Error procesando XML: {file}. Error: {ex.Message}");
                return false;
            }
        }

        static async Task<DataTable> PrepareCopyDataTable(string json)
        {
            var result = json;
            var tableName = "FacturasConceptosXML";
            DataTable dataTableCopy = new DataTable();
            dataTableCopy.TableName = "FacturasConceptosXML";
            try
            {
                string dataSetTemplate = $"{{\"{tableName}\": [{result.Replace("[", "").Replace("]", "")}]}}";
                DataSet _dataSet = JsonConvert.DeserializeObject<DataSet>(dataSetTemplate);

                dataTableCopy = _dataSet.Tables[tableName];
            }
            catch (Exception ex)
            {
                //Logger.Error(ex.Message);
                return new DataTable();
            }

            return dataTableCopy;
        }


        private decimal ParseDecimal(string value)
        {
            return decimal.TryParse(value, out var result) ? result : 0;
        }
    }

    public class PathSettings
    {
        public List<string> PathFacturasXML { get; set; } = new();
    }
}
