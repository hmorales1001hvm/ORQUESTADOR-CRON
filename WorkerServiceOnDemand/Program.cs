using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Soltec.Orquestacion.BR;
using Soltec.Orquestacion.BR.Entities;
using WorkerServiceOnDemand;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "SOLTEC - Worker Service OnDemand";
});

builder.Services.Configure<ApiSettings>(
    builder.Configuration.GetSection("ApiSettings"));

builder.Services.AddHttpClient();

// 🔧 CAMBIO AQUÍ
builder.Services.AddScoped<OnDemand>();
builder.Services.AddScoped<Historicos>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
await host.RunAsync();
