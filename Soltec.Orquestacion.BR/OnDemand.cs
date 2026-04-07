using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Soltec.Orquestacion.DA.Entities;
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
            var servers = await DA.Orchestration.LoadServersDB();

            foreach (var baseUrl in _apiSettings.Urls)
            {
                try
                {
                    var client = _httpClientFactory.CreateClient();

                    // Para descargas grandes
                    client.Timeout = Timeout.InfiniteTimeSpan;

                    var url = $"{baseUrl.TrimEnd('/')}/venta/DescargarOnDemandZip";

                    _logger.LogInformation("Consumiendo API: {url}", url);

                    using var response = await client.GetAsync(
                        url,
                        HttpCompletionOption.ResponseContentRead, // 👈 explícito y estable
                        cancellationToken
                    );

                    if (!response.IsSuccessStatusCode)
                    {
                        var error = await response.Content.ReadAsStringAsync(cancellationToken);

                        _logger.LogWarning(
                            "Error al consumir {url}. Status: {status}. Response: {error}",
                            url,
                            response.StatusCode,
                            error
                        );

                        continue;
                    }

                    // Validación extra (evita nulls raros)
                    if (response.Content == null)
                    {
                        _logger.LogWarning("Respuesta sin contenido en {url}", url);
                        continue;
                    }

                    await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);

                    if (zipStream == null || zipStream.Length == 0)
                    {
                        _logger.LogWarning("Stream vacío en {url}", url);
                        continue;
                    }

                    await ProcesarZipDesdeStream(
                        zipStream,
                        baseUrl,
                        cancellationToken,
                        servers
                    );

                    _logger.LogInformation("Procesamiento OnDemand finalizado desde {url}", url);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Descarga OnDemand cancelada para {url}", baseUrl);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error procesando URL {url}", baseUrl);
                }
            }
        }

        private async Task ProcesarZipDesdeStream(Stream zipStream, string baseUrl, CancellationToken cancellationToken, List<OrquestadorServidorMySQL> orquestadorServidorMySQLs)
        {
            using var outerArchive = new ZipArchive(zipStream, ZipArchiveMode.Read);

            foreach (var zipEntry in outerArchive.Entries)
            {
                if (!zipEntry.Name.EndsWith("_DatosOnDemand.zip", StringComparison.OrdinalIgnoreCase))
                    continue;

                _logger.LogInformation("Procesando ZIP interno: {zip}", zipEntry.Name);

                await using var innerZipStream = zipEntry.Open();

                await ProcesarZipInternoDesdeStream(
                    innerZipStream,
                    baseUrl,
                    zipEntry.Name,
                    cancellationToken, orquestadorServidorMySQLs
                );
            }
        }

        private async Task ProcesarZipInternoDesdeStream(Stream zipStream, string baseUrl, string zipName,  CancellationToken cancellationToken, List<OrquestadorServidorMySQL> orquestadorServidorMySQLs)
        {
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);

            ConectDB conectDB = null;
            OnDemandDTO salesDataDto = null;

            string clave = ObtenerClaveDesdeZip(archive);

            foreach (var entry in archive.Entries)
            {
                if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                    continue;

                await using var entryStream = entry.Open();
                using var reader = new StreamReader(entryStream);

                if (entry.Name.Equals($"{clave}_infoDB.json", StringComparison.OrdinalIgnoreCase))
                {
                    conectDB = JsonConvert.DeserializeObject<ConectDB>(await reader.ReadToEndAsync());
                }
                else if (entry.Name.Equals($"{clave}_data.json", StringComparison.OrdinalIgnoreCase))
                {
                    salesDataDto = JsonConvert.DeserializeObject<OnDemandDTO>(await reader.ReadToEndAsync());
                }
            }

            if (conectDB == null || salesDataDto == null)
            {
                _logger.LogError("ZIP interno inválido: {zip}", zipName);
                return;
            }

            var server = orquestadorServidorMySQLs.Where(x => x.ClaveSimi == clave).FirstOrDefault();
            if (server!=null)
                await Soltec.Orquestacion.DA.Orchestration.SincronizaOnDemand(conectDB, salesDataDto, clave, server);

            await EliminarZipRemoto(baseUrl, zipName, cancellationToken);

            _logger.LogInformation("ZIP interno procesado correctamente: {zip}", zipName);
        }


        private async Task EliminarZipRemoto(string baseUrl, string fileName, CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();

            if (!baseUrl.EndsWith("/"))
                baseUrl += "/";

            var url = $"{baseUrl}venta/EliminarOnDemandZip?fileName={Uri.EscapeDataString(fileName)}";

            _logger.LogInformation("Solicitando eliminación del ZIP: {url}", url);

            using var response = await client.PostAsync(url, null, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync(cancellationToken);

                _logger.LogWarning(
                    "No se pudo eliminar el ZIP remoto {file}. Status: {status}. Response: {response}",
                    fileName,
                    response.StatusCode,
                    content);
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
