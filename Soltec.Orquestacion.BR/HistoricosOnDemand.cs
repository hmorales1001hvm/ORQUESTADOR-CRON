using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Soltec.Orquestacion.DA.Entities;
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
    public class HistoricosOnDemand
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<Historicos> _logger;
        private readonly ApiSettings _apiSettings;
        const int TAM_BLOQUE = 5;          
        const int BLOQUES_PARALELOS = 10;   

        public HistoricosOnDemand(IHttpClientFactory httpClientFactory, ILogger<Historicos> logger, IOptions<ApiSettings> apiSettings)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _apiSettings = apiSettings.Value;
        }

        public async Task<bool> ProcesaHistoricos(CancellationToken ct)
        {
            var recibidos = await DA.Orchestration.CargaHistoricosRecibidosOnDemand();
            if (recibidos.Count > 0)
            {

                var servers = await DA.Orchestration.LoadServersDB();

                var bloques = recibidos.ChunkBy(TAM_BLOQUE);

                var options = new ParallelOptions
                {
                    MaxDegreeOfParallelism = BLOQUES_PARALELOS,
                    CancellationToken = ct
                };

                _logger.LogInformation($"Procesando por bloques: {BLOQUES_PARALELOS}");
                await Parallel.ForEachAsync(bloques, options, async (bloque, token) =>
                {
                    var tomados = new List<ModelTransmisiones>();

                    // 🔒 Toma lógica
                    foreach (var item in bloque)
                    {
                        var tomado = await DA.Orchestration.ActualizarEstatus(item.Id, item.Clave, "PROCESANDO...",
                            "RECIBIDO");

                        if (tomado)
                            tomados.Add(item);

                        _logger.LogInformation($"Procesando {item.Clave}, estatus: PROCESANDO...");
                    }

                    // 🧠 Procesamiento SECUENCIAL del bloque
                    foreach (var item in tomados)
                    {
                        try
                        {
                            await ProcesarHistorico(item, token, servers);

                            await DA.Orchestration.ActualizarEstatus(item.Id, item.Clave, "PROCESADO", "PROCESANDO...");
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error procesando histórico {Clave}", item.Clave);

                            await DA.Orchestration.ActualizarEstatus(item.Id, item.Clave, "RECIBIDO", "PROCESANDO...");
                        }
                    }
                });
            }
            return true;
        }



        private async Task ProcesarHistorico(ModelTransmisiones item,  CancellationToken cancellationToken, List<OrquestadorServidorMySQL> orquestadorServidorMySQLs)
        {
            byte[] fileBytes = null;
            string uri = string.Empty;

            foreach (var url in _apiSettings.Urls)
            {
                try
                {
                    var client = _httpClientFactory.CreateClient();
                    client.BaseAddress = new Uri(url.EndsWith("/") ? url : url + "/");

                    var endpoint = new Uri(
                        client.BaseAddress,
                        $"venta/DescargarScriptZipOnDemand?sucursal={item.Clave}");

                    var response = await client.GetAsync(endpoint, cancellationToken);

                    if (response.IsSuccessStatusCode)
                    {
                        uri = url;
                        fileBytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                        break;
                    } else
                    {
                        if (!response.IsSuccessStatusCode)
                        {
                            var error = await response.Content.ReadAsStringAsync();
                            Console.WriteLine($"Error en {url}: {response.StatusCode} - {error}");
                        }
                    }
                }
                catch { }
            }

            if (fileBytes == null)
                throw new Exception($"No se pudo descargar ZIP para {item.Clave}");

            using var memoryStream = new MemoryStream(fileBytes);
            using var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read);

            ConectDB transmisionHistorico = null;
            OnDemandDTO salesDataDto = null;

            foreach (var entry in archive.Entries)
            {
                if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                    continue;

                using var reader = new StreamReader(entry.Open());
                var json = await reader.ReadToEndAsync();

                if (entry.Name == $"{item.Clave}_infoDB.json")
                    transmisionHistorico = JsonConvert.DeserializeObject<ConectDB>(json);
                else if (entry.Name == $"{item.Clave}_data.json")
                    salesDataDto = JsonConvert.DeserializeObject<OnDemandDTO>(json);
            }

            if (transmisionHistorico == null || salesDataDto == null)
                throw new Exception($"ZIP inválido para {item.Clave}");
            var server = orquestadorServidorMySQLs.Where(x => x.ClaveSimi == item.Clave).FirstOrDefault();

            await Soltec.Orquestacion.DA.Orchestration.SincronizaHistoricosOnDemand(salesDataDto, item.Clave, server);

            string zipName = $"{item.Clave}_DatosHistoricosOnDemand.zip";

            await EliminarZipRemoto(uri, zipName, cancellationToken);
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

    }
}
