using Microsoft.Extensions.Options;
using Soltec.Orquestacion.BR;

namespace WorkerServiceFacturasXML
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<Worker> _logger;
        private readonly PathSettings _pathSettings;

        private static readonly SemaphoreSlim _facturasXmlLock = new(1, 1);

        public Worker(
            ILogger<Worker> logger,
            IServiceScopeFactory scopeFactory,
            IOptions<PathSettings> pathSettings)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _pathSettings = pathSettings.Value;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker Facturas XML iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (!_pathSettings.PathFacturasXML.Any())
                    {
                        _logger.LogWarning("No hay rutas configuradas. Reintentando en 10 minutos...");
                    }
                    else
                    {
                        var ruta = _pathSettings.PathFacturasXML.First();

                        using var scope = _scopeFactory.CreateScope();
                        var facturasXml = scope.ServiceProvider
                            .GetRequiredService<FacturasXML>();

                        await EjecutarFacturasXml(facturasXml, ruta, stoppingToken);
                    }

                    await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Worker Facturas XML detenido.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error inesperado en el Worker Facturas XML");
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                }
            }
        }

        private async Task EjecutarFacturasXml(FacturasXML facturasXml, string ruta, CancellationToken ct)
        {
            if (!await _facturasXmlLock.WaitAsync(0, ct))
            {
                _logger.LogWarning("Facturas XML sigue en ejecución. Se omite este ciclo.");
                return;
            }

            try
            {
                _logger.LogInformation("Facturas XML iniciado.");
                await facturasXml.ProcesaFacturasConceptosXML(ruta, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en Facturas XML");
            }
            finally
            {
                _facturasXmlLock.Release();
                _logger.LogInformation("Facturas XML finalizado.");
            }
        }
    }
}
