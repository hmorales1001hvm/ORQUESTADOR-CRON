using Serilog;
using Soltec.Orquestacion.BR;
using WorkerServiceHistoricos;

try
{
    // ------------------------
    // Crear carpeta de logs
    // ------------------------
    var logDir = Path.Combine(AppContext.BaseDirectory, "Logs");
    if (!Directory.Exists(logDir))
        Directory.CreateDirectory(logDir);

    var logFilePath = Path.Combine(logDir, "WorkerServiceHistoricos-.log");

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

    Log.Information("Iniciando Worker Service Históricos...");

    // ------------------------
    // Crear host
    // ------------------------
    var host = Host.CreateDefaultBuilder(args)
        .UseWindowsService(options =>
        {
            options.ServiceName = "SOLTEC - Worker Service Históricos";
        })
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            // Configuración
            services.Configure<ApiSettings>(
                context.Configuration.GetSection("ApiSettings"));

            // HttpClient
            services.AddHttpClient();

            // Servicios internos
            services.AddScoped<Historicos>();

            // Worker
            services.AddHostedService<Worker>();
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Error crítico al iniciar el servicio Históricos.");
}
finally
{
    Log.CloseAndFlush();
}
