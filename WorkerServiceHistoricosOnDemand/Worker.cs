using Microsoft.Extensions.DependencyInjection;
using Soltec.Orquestacion.BR;

namespace WorkerServiceHistoricos
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<Worker> _logger;

        private static readonly SemaphoreSlim _lock = new(1, 1);

        public Worker(
            ILogger<Worker> logger,
            IServiceScopeFactory scopeFactory)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker Históricos ON DEMAND iniciado.");

            using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    if (!await _lock.WaitAsync(0))
                    {
                        _logger.LogWarning("Históricos aún en ejecución. Ciclo omitido.");
                        continue;
                    }

                    await EjecutarAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Apagado normal del servicio
            }
        }

        private async Task EjecutarAsync(CancellationToken ct)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var historicos = scope.ServiceProvider
                    .GetRequiredService<HistoricosOnDemand>();

                _logger.LogInformation("Históricos iniciado.");
                await historicos.ProcesaHistoricos(ct);
                _logger.LogInformation("Históricos finalizado.");
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Históricos cancelado por apagado del servicio.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en Históricos");
            }
            finally
            {
                _lock.Release();
            }
        }
    }

}
