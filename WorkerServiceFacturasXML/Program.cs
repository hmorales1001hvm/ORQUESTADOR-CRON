using Serilog;
using Soltec.Orquestacion.BR;
using WorkerServiceFacturasXML;

try
{
    var logDir = Path.Combine(AppContext.BaseDirectory, "Logs");
    Directory.CreateDirectory(logDir);

    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Information()
        .Enrich.FromLogContext()
        .WriteTo.Console()
        .WriteTo.File(
            path: Path.Combine(logDir, "WorkerServiceFacturasXML-.log"),
            rollingInterval: RollingInterval.Day,
            retainedFileCountLimit: 30
        )
        .CreateLogger();

    Log.Information("Iniciando Worker Service Facturas XML...");

    var host = Host.CreateDefaultBuilder(args)
        .UseWindowsService(o =>
        {
            o.ServiceName = "SOLTEC - Worker Service Facturas XML";
        })
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            services.AddOptions<PathSettings>()
                .Bind(context.Configuration.GetSection("Path"))
                .ValidateOnStart();

            services.AddScoped<FacturasXML>();
            services.AddHostedService<Worker>();
        })
        .Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Error crítico al iniciar el servicio Facturas XML.");
}
finally
{
    Log.CloseAndFlush();
}
