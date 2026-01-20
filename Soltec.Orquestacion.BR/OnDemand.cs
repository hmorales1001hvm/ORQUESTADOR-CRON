using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Soltec.Orquestacion.Entidades;
using Soltec.Orquestacion.Entidades.DTOs;
using System.IO.Compression;

namespace Soltec.Orquestacion.BR
{
    public class OnDemand
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<OnDemand> _logger;
        private readonly ApiSettings _apiSettings;

        public OnDemand(IHttpClientFactory httpClientFactory, ILogger<OnDemand> logger, IOptions<ApiSettings> apiSettings)
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
                    var url = $"{baseUrl}venta/DescargarOnDemandZip";

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

                    await ProcesarZipEnMemoria(fileBytes, baseUrl, cancellationToken);

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error procesando URL {url}", baseUrl);
                }
            }
        }

        private async Task ProcesarZipEnMemoria(byte[] fileBytes, string baseUrl, CancellationToken cancellationToken)
        {
            using var memoryStream = new MemoryStream(fileBytes);
            using var outerArchive = new ZipArchive(memoryStream, ZipArchiveMode.Read);

            foreach (var zipEntry in outerArchive.Entries)
            {
                if (!zipEntry.Name.EndsWith("_DatosOnDemand.zip", StringComparison.OrdinalIgnoreCase))
                    continue;

                _logger.LogInformation("Procesando ZIP interno: {zip}", zipEntry.Name);

                using var innerZipStream = zipEntry.Open();
                using var innerMemoryStream = new MemoryStream();
                await innerZipStream.CopyToAsync(innerMemoryStream, cancellationToken);

                innerMemoryStream.Position = 0;

                await ProcesarZipInterno(innerMemoryStream, baseUrl, zipEntry.Name, cancellationToken);
            }
        }

        private async Task ProcesarZipInterno(MemoryStream zipStream,  string baseUrl,  string zipName, CancellationToken cancellationToken)
        {
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);

            ConectDB conectDB = null;
            OnDemandDTO salesDataDto = null;

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
                    conectDB = JsonConvert.DeserializeObject<ConectDB>(jsonContent);
                }
                else if (entry.Name.Equals($"{clave}_data.json", StringComparison.OrdinalIgnoreCase))
                {
                    salesDataDto = JsonConvert.DeserializeObject<OnDemandDTO>(jsonContent);
                }
            }

            if (conectDB == null || salesDataDto == null)
            {
                _logger.LogError("ZIP interno inválido: {zip}", zipName);
                return;
            }

            // 🔥 PROCESO PRINCIPAL
            await Soltec.Orquestacion.DA.Orchestration.SincronizaOnDemand(conectDB, salesDataDto, clave);

            // 🧹 BORRAR ZIP REMOTO
            await EliminarZipRemoto(baseUrl, zipName, cancellationToken);

            _logger.LogInformation("ZIP interno procesado correctamente: {zip}", zipName);
        }



        private async Task EliminarZipRemoto(string baseUrl, string fileName,  CancellationToken cancellationToken)
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


        private string ObtenerClaveDesdeZip(ZipArchive archive)
        {
            var entry = archive.Entries
                .FirstOrDefault(e => e.Name.EndsWith("_infoDB.json", StringComparison.OrdinalIgnoreCase));

            return entry?.Name.Split('_')[0];
        }


    }

    public class ApiSettings
    {
        public List<string> Urls { get; set; } = new();
    }
}
