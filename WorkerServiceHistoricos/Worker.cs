using Microsoft.Extensions.DependencyInjection;
using Soltec.Orquestacion.BR;

namespace WorkerServiceHistoricos
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<Worker> _logger;

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
            _logger.LogInformation("Worker Históricos iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                using var scope = _scopeFactory.CreateScope();
                var historicos = scope.ServiceProvider.GetRequiredService<Historicos>();

                await EjecutarHistoricos(historicos, stoppingToken);

                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
            }
        }

        private async Task EjecutarHistoricos(Historicos historicos, CancellationToken ct)
        {
            if (!await _historicosLock.WaitAsync(0, ct))
            {
                _logger.LogWarning("Históricos sigue en ejecución. Se omite este ciclo.");
                return;
            }

            try
            {
                _logger.LogInformation("Históricos iniciado.");
                await historicos.ProcesaHistoricos(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en Históricos");
            }
            finally
            {
                _historicosLock.Release();
                _logger.LogInformation("Históricos finalizado.");
            }
        }
    }
}
