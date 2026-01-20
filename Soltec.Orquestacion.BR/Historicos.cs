using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Soltec.Orquestacion.Entidades;
using Soltec.Orquestacion.Entidades.DTOs;
using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.BR
{
    public class Historicos
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<Historicos> _logger;
        private readonly ApiSettings _apiSettings;

        public Historicos(IHttpClientFactory httpClientFactory, ILogger<Historicos> logger, IOptions<ApiSettings> apiSettings)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _apiSettings = apiSettings.Value;
        }
        public async Task<bool> ProcesaHistoricos(CancellationToken cancellationToken)
        {
            var resultado = await Soltec.Orquestacion.DA.Orchestration.CargaHistoricosRecibidos();
            string uri = string.Empty;

            foreach (var item in resultado)
            {
                byte[] fileBytes = null;

                foreach (var url in _apiSettings.Urls)
                {
                    try
                    {
                        using var client = new HttpClient();
                        client.BaseAddress = new Uri(url.EndsWith("/") ? url : url + "/");

                        var endpoint = new Uri(
                            client.BaseAddress,
                            $"venta/DescargarScriptZip?sucursal={item.Clave}");

                        var response = await client.GetAsync(endpoint, cancellationToken);

                        if (response.IsSuccessStatusCode)
                        {
                            uri = url;
                            fileBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                            break;
                        }
                    }
                    catch { }
                }

                if (fileBytes == null)
                {
                    _logger.LogError("No se pudo descargar el ZIP para {clave}", item.Clave);
                    continue;
                }

                using var memoryStream = new MemoryStream(fileBytes);
                using var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read);

                ConectDB transmisionHistorico = null;
                SalesDataDto salesDataDto = null;

                foreach (var entry in archive.Entries)
                {
                    if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                        continue;

                    using var reader = new StreamReader(entry.Open());
                    var json = await reader.ReadToEndAsync();

                    if (entry.Name == $"{item.Clave}_infoDB.json")
                        transmisionHistorico = JsonConvert.DeserializeObject<ConectDB>(json);
                    else if (entry.Name == $"{item.Clave}_data.json")
                        salesDataDto = JsonConvert.DeserializeObject<SalesDataDto>(json);
                }

                if (transmisionHistorico == null || salesDataDto == null)
                {
                    _logger.LogError("ZIP inválido para {clave}", item.Clave);
                    continue;
                }

                await Soltec.Orquestacion.DA.Orchestration
                    .SincronizaHistoricos(transmisionHistorico, salesDataDto, item.Clave, item.Id);

                string zipName = $"{item.Clave}_DatosHistoricos.zip";

                await EliminarZipRemoto(uri, zipName, cancellationToken);
            }

            return true;
        }




        private async Task EliminarZipRemoto(string baseUrl, string fileName, CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();
            var url = $"{baseUrl}venta/EliminarOnDemandZip?fileName={Uri.EscapeDataString(fileName)}";

            _logger.LogInformation("Solicitando eliminación del ZIP: {url}", url);

            using var response = await client.DeleteAsync(url, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "No se pudo eliminar el ZIP remoto {file}. Status: {status}",
                    fileName,
                    response.StatusCode);
            }
        }

    }
}
