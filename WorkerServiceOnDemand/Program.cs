using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Soltec.Orquestacion.BR;
using Soltec.Orquestacion.BR.Entities;
using WorkerServiceOnDemand;
using System;
using System.IO;

try
{
    // ------------------------
    // Crear carpeta de logs
    // ------------------------
    var logDir = Path.Combine(AppContext.BaseDirectory, "Logs");
    if (!Directory.Exists(logDir))
        Directory.CreateDirectory(logDir);

    var logFilePath = Path.Combine(logDir, "WorkerService-.log");

    // ------------------------
    // Configurar Serilog
    // ------------------------
    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Information()
        .Enrich.FromLogContext()
        .WriteTo.Console()
        .WriteTo.File(
            path: logFilePath,
            rollingInterval: RollingInterval.Day,
            retainedFileCountLimit: 30,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level:u3}] {Message:lj}{NewLine}{Exception}"
        )
        .CreateLogger();

    Log.Information("Iniciando Worker Service OnDemand...");

    // ------------------------
    // Crear host
    // ------------------------
    var host = Host.CreateDefaultBuilder(args)
        .UseWindowsService(options =>
        {
            options.ServiceName = "SOLTEC - Worker Service OnDemand";
        })
        .UseSerilog() // Integrar Serilog con el host
        .ConfigureServices((context, services) =>
        {
            // Configuración desde appsettings.json
            services.Configure<ApiSettings>(
                context.Configuration.GetSection("ApiSettings"));

            // HttpClient
            services.AddHttpClient();

            // Servicios internos
            services.AddScoped<OnDemand>();
            services.AddScoped<Historicos>();

            // Worker principal
            services.AddHostedService<Worker>();
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Error crítico al iniciar el servicio.");
}
finally
{
    Log.CloseAndFlush();
}
