using Soltec.Orquestacion.BR;

namespace WorkerServiceOnDemand
{
    public class Worker : BackgroundService
    {
        private readonly OnDemand _onDemand;
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger, OnDemand onDemand)
        {
            _logger = logger;
            _onDemand = onDemand;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker ejecutando: {time}", DateTimeOffset.Now);

                await _onDemand.DescargarDatosOnDemand(stoppingToken);

                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
            }
        }
    }
}
