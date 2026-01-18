using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Soltec.Orquestacion.Entidades;
using Soltec.Orquestacion.Entidades.DTOs;
using System.IO.Compression;
using System.Net.Http;

namespace Soltec.Orquestacion.BR
{
    public class OnDemand
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<OnDemand> _logger;
        private readonly ApiSettings _apiSettings;

        public OnDemand(
            IHttpClientFactory httpClientFactory,
            ILogger<OnDemand> logger,
            IOptions<ApiSettings> apiSettings)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _apiSettings = apiSettings.Value;
        }

        public async Task DescargarDatosOnDemand(CancellationToken cancellationToken)
        {
            foreach (var baseUrl in _apiSettings.Urls)
            {
                try
                {
                    var client = _httpClientFactory.CreateClient();
                    var url = $"{baseUrl}venta/DescargarOnDemandZip?sucursal=TODAS";

                    _logger.LogInformation("Consumiendo API: {url}", url);

                    using var response = await client.GetAsync(url, cancellationToken);

                    if (!response.IsSuccessStatusCode)
                    {
                        _logger.LogWarning("Error al consumir {url}. Status: {status}", url, response.StatusCode);
                        continue;
                    }

                    var fileBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);

                    if (fileBytes == null || fileBytes.Length == 0)
                    {
                        _logger.LogError("No se pudo descargar el archivo desde {url}", url);
                        continue;
                    }

                    await ProcesarZipEnMemoria(fileBytes, baseUrl);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error procesando URL {url}", baseUrl);
                }
            }
        }




        private async Task ProcesarZipEnMemoria(byte[] fileBytes, string baseUrl)
        {
            using var memoryStream = new MemoryStream(fileBytes);
            using var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read);

            TransmisionHistorico transmisionHistorico = null;
            SalesDataDto salesDataDto = null;

            string clave = ObtenerClaveDesdeZip(archive);

            foreach (var entry in archive.Entries)
            {
                if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                    continue;

                using var entryStream = entry.Open();
                using var reader = new StreamReader(entryStream);
                string jsonContent = await reader.ReadToEndAsync();

                if (entry.Name.Equals($"{clave}_infoDB.json", StringComparison.OrdinalIgnoreCase))
                {
                    transmisionHistorico = JsonConvert.DeserializeObject<TransmisionHistorico>(jsonContent);
                }
                else if (entry.Name.Equals($"{clave}_data.json", StringComparison.OrdinalIgnoreCase))
                {
                    salesDataDto = JsonConvert.DeserializeObject<SalesDataDto>(jsonContent);
                }
            }

            if (transmisionHistorico == null || salesDataDto == null)
            {
                _logger.LogError("El ZIP no contiene los archivos JSON esperados. Clave={clave}", clave);
                return;
            }

            _logger.LogInformation("ZIP válido para clave {clave}. Procesando...", clave);

            await Soltec.Orquestacion.DA.Orchestration.SincronizaHistoricos(transmisionHistorico, salesDataDto, clave, 0);
        }


        private string ObtenerClaveDesdeZip(ZipArchive archive)
        {
            var entry = archive.Entries
                .FirstOrDefault(e => e.Name.EndsWith("_infoDB.json"));

            return entry?.Name.Split('_')[0];
        }



    }

    public class ApiSettings
    {
        public List<string> Urls { get; set; } = new();
    }
}
