using Microsoft.Extensions.DependencyInjection;
using Soltec.Orquestacion.BR;
using Soltec.Orquestacion.BR.Entities;

namespace WorkerServiceOnDemand
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<Worker> _logger;

        private static readonly SemaphoreSlim _onDemandLock = new(1, 1);
        private static readonly SemaphoreSlim _historicosLock = new(1, 1);

        public Worker(
            ILogger<Worker> logger,
            IServiceScopeFactory scopeFactory)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Iniciando ciclo: {time}", DateTimeOffset.Now);

                using var scope = _scopeFactory.CreateScope();

                var onDemand = scope.ServiceProvider.GetRequiredService<OnDemand>();
                var historicos = scope.ServiceProvider.GetRequiredService<Historicos>();

                var onDemandTask = EjecutarOnDemand(onDemand, stoppingToken);
                var historicosTask = EjecutarHistoricos(historicos, stoppingToken);

                await Task.WhenAll(onDemandTask, historicosTask);

                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
            }
        }

        private async Task EjecutarOnDemand(OnDemand onDemand, CancellationToken ct)
        {
            if (!await _onDemandLock.WaitAsync(0, ct))
            {
                _logger.LogWarning("OnDemand sigue en ejecución. Se omite este ciclo.");
                return;
            }

            try
            {
                _logger.LogInformation("OnDemand iniciado.");
                await onDemand.DescargarDatosOnDemand(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en OnDemand");
            }
            finally
            {
                _onDemandLock.Release();
                _logger.LogInformation("OnDemand finalizado.");
            }
        }

        private async Task EjecutarHistoricos(Historicos historicos, CancellationToken ct)
        {
            if (!await _historicosLock.WaitAsync(0, ct))
            {
                _logger.LogWarning("Historicos sigue en ejecución. Se omite este ciclo.");
                return;
            }

            try
            {
                _logger.LogInformation("Historicos iniciado.");
                await historicos.ProcesaHistoricos(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en Historicos");
            }
            finally
            {
                _historicosLock.Release();
                _logger.LogInformation("Historicos finalizado.");
            }
        }
    }
}
