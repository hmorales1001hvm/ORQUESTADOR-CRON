using Microsoft.Extensions.Configuration;
using OrquestadorCRON;
using Soltec.Common.Logger;
using Soltec.Orquestacion.BR;
using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace Soltec.Orquestacion.App
{
    public class Program
    {
        private static Settings1 Settings;
        private static string[] ApiUrls;

        public static async Task Main(string[] args)
        {
            try
            {
                var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory) 
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

                Settings = configuration.GetSection("Settings1").Get<Settings1>();
                ApiUrls = configuration.GetSection("ApiSettings:Urls").Get<string[]>();

                if (Settings == null)
                    throw new Exception("No se pudo cargar Settings1 desde appsettings.json");

                if (ApiUrls == null || ApiUrls.Length == 0)
                    throw new Exception("ApiSettings:Urls no está configurado.");

                await RunAsync(args);
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
        }

        private static async Task RunAsync(string[] args)
        {
            var processNumber = args.Length > 0 ? int.Parse(args[0]) : 0;
            var idEmpresa = 0;
            var valorInicial = 0;
            var valorFinal = 0;
            var peticion = 0;
            var arrayEmpresas = string.Empty;

            string logName = $"\\Proceso-{processNumber}\\";

            if (processNumber == 1)
            {
                idEmpresa = int.Parse(args[1]);
                valorInicial = int.Parse(args[2]);
                valorFinal = int.Parse(args[3]);
                logName = $"\\Proceso-{processNumber}_{idEmpresa}_{valorInicial}_{valorFinal}\\";
            }
            else if (processNumber == 4)
            {
                peticion = int.Parse(args[1]);
            }
            else if (processNumber == 7)
            {
                arrayEmpresas = args[1];
            }
            else if (processNumber == 11)
            {
                idEmpresa = int.Parse(args[1]);
                logName = $"\\Proceso-{processNumber}_{idEmpresa}\\";
            }

            Logger.Info($"No. de proceso: {processNumber}");
            FileUtil.localLogPath = $"{Settings.LogPath}{logName}";

            switch (processNumber)
            {
                case 0:
                    Logger.Important("========== Iniciando replicación de BD ==========");
                    await Orchestration.OrquestacionDB();
                    break;

                case 1:
                    Logger.Important("========== Iniciando orquestación ==========");
                    await Orchestration.ProcessFiles(
                        Settings.PathSourceFile + "Operativas",
                        idEmpresa,
                        valorInicial,
                        valorFinal);
                    break;

                case 2:
                    Logger.Important("========== Orquestación catálogos ==========");
                    await Orchestration.ProcessFilesCatalogs(
                        Settings.PathSourceFile + "Catalogos");
                    break;

                case 3:
                    Logger.Important("========== Actualización catálogos ==========");
                    await Orchestration.LoadUpdateCatalogSQLServerAsync();
                    break;

                case 4:
                    Logger.Important("========== API Recibe Ticket ==========");
                    await Orchestration.RecibeTicket(
                        Settings.PathFileJSON,
                        peticion,
                        Settings.ApiRecibeTicket);
                    break;

                case 5:
                    Logger.Important("========== Backup Tickets ==========");
                    await Orchestration.BackupTicketsAsync();
                    break;

                case 6:
                    Logger.Important("========== Backup Bucket Contabo ==========");
                    await Orchestration.BackupBucketContaboAsync(
                        Settings.AccessKey,
                        Settings.SecretKey,
                        Settings.BucketName,
                        Settings.ServiceBucketURL);
                    break;

                case 7:
                    Logger.Important("========== Ventas Depósitos ==========");
                    await Orchestration.ProcesaDatosVentaDepositos(arrayEmpresas);
                    break;

                case 8:
                    Logger.Important("========== Lectura XML ==========");
                    await Orchestration.ProcesaFacturasConceptosXML(
                        Settings.RutaFacturasXML);
                    break;

                case 9:
                    Logger.Important("========== Kushki ==========");
                    await Orchestration.ProcesaKushki(
                        Settings.KushkiHost,
                        Settings.KushkiUserName,
                        Settings.KushkiPathFilePEM,
                        Settings.KushkiRemoteFilePath,
                        Settings.KushkiPathDownloadFile);
                    break;

                case 10:
                    Logger.Important("========== Monitoreo ==========");
                    await Orchestration.MonitoreoArchivos(Settings.ApiURL);
                    break;

                case 11:
                    var version = Assembly.GetExecutingAssembly().GetName().Version;
                    Logger.Important($"========== SQS AWS [{version}] ==========");
                    await Orchestration.ProcesaSQS(
                        Settings.AccessKeySQS,
                        Settings.SecretKeySQS,
                        Settings.RegionSQS,
                        idEmpresa);
                    break;

                case 12:
                    var version2 = Assembly.GetExecutingAssembly().GetName().Version;
                    Logger.Important($"========== Históricos [{version2}] ==========");
                    await Orchestration.ProcesaHistoricos(ApiUrls);
                    break;
                case 13:
                    var versionSIMIPET = Assembly.GetExecutingAssembly().GetName().Version;
                    Logger.Important($"========== Históricos [{versionSIMIPET}] ==========");
                    await Orchestration.ProcesaHistoricosSIMIPET(ApiUrls);
                    break;

                default:
                    Logger.Info("Proceso no reconocido.");
                    break;
            }
        }
    }
}
