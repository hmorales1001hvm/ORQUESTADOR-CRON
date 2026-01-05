using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using ClosedXML.Excel;
using DocumentFormat.OpenXml.Office2010.Excel;
using DocumentFormat.OpenXml.Wordprocessing;
using Newtonsoft.Json;
using Renci.SshNet;
using Soltec.Common.Logger;
using Soltec.Orquestacion.BR.Entities;
using Soltec.Orquestacion.DA.Entities;
using Soltec.Orquestacion.Entidades;
using Soltec.Orquestacion.Entidades.DTOs;
using System.Data;
using System.Diagnostics;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Xml.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;
//using Amazon.S3.Model;

namespace Soltec.Orquestacion.BR
{
    public class Orchestration
    {
        public static DataSet dataSet = new DataSet();
        public static int fileCount = 0;
        DA.Orchestration orchestration = new DA.Orchestration();
        public static List<Conceptos> conceptos = new List<Conceptos>();
        private readonly IHttpClientFactory _httpClientFactory;


        public Orchestration(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        public static async Task<bool> ProcesaDatosVentaDepositos(string arrayEmpresas)
        {
            var result = new Soltec.Orquestacion.DA.Orchestration().ProcesaDatosVentaDepositos(arrayEmpresas).Result;
            return result;
        }


        public static async Task<bool> MonitoreoArchivos(string urlAPI)
        {
            try
            {
                var apiUrl = $"{urlAPI}/transmision/getMonitorDeApps";

                var apiResponse = new ApiResponse<MonitorDeApps>();
                HttpResponseMessage response;

                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Clear();
                    response = client.GetAsync(apiUrl).Result;

                    if ((int)response.StatusCode == 503 || (int)response.StatusCode == 401)
                    {
                        apiResponse.Success = false;
                        apiResponse.Message = response.ReasonPhrase;
                    }

                    apiResponse = JsonConvert.DeserializeObject<ApiResponse<MonitorDeApps>>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));
                    foreach (var file in apiResponse.List.ToList())
                    {
                        if (IsProcessRunning($"{file.NombreProceso}"))
                        {
                            Logger.Info($"La aplicación {file.NombreEXE} se encuentra en ejecución .");
                        }
                        else
                        {
                            // Verifica que exista el archivo
                            if (!System.IO.File.Exists($"{file.Ruta}{file.NombreEXE}"))
                            {
                                Logger.Warning($"❌ No se encontró la aplicación en: " + $"{file.Ruta}{file.NombreEXE}");
                            }
                            else
                            {

                                try
                                {
                                    string rutaCompleta = Path.Combine(file.Ruta, file.NombreEXE);

                                    if (File.Exists(rutaCompleta))
                                    {
                                        var psi = new ProcessStartInfo
                                        {
                                            FileName = rutaCompleta,
                                            UseShellExecute = false,
                                            Verb = "runas", // ← Esto es clave para elevar privilegios
                                            WindowStyle = ProcessWindowStyle.Normal,
                                            CreateNoWindow = false
                                        };

                                        System.Diagnostics.Process.Start(psi);
                                        Logger.Info($"La aplicación {rutaCompleta} se ha iniciado correctamente.");
                                    }
                                    else
                                    {
                                        Logger.Info("No se encontró el archivo: " + rutaCompleta);
                                    }

                                }
                                catch (Exception ex)
                                {
                                    Logger.Error($"⚠️ Error al iniciar la aplicación {file.NombreEXE}: \n{ex.Message}");
                                }
                            }
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                throw;
            }

            return true;
        }


        static bool IsProcessRunning(string processName)
        {
            bool running = false;
            Process[] processes = System.Diagnostics.Process.GetProcessesByName(processName);
            if (processes.Length > 0)
            {
                foreach (var p in processes)
                {
                    if (p.ProcessName == processName)
                    {
                        running = true;
                    }
                }
                return running;
            }
            else
            {
                return running;
            }
        }


        public static async Task<bool> ProcesaKushki(string host, string userName, string privateKeyPath, string remoteFilePath, string pathDownloadFile)
        {
            var keyFile = new PrivateKeyFile(privateKeyPath);
            var keyFiles = new[] { keyFile };
            var authMethod = new PrivateKeyAuthenticationMethod(userName, keyFiles);
            var connectionInfo = new ConnectionInfo(host, 22, userName, authMethod);

            if (!Directory.Exists(pathDownloadFile))
                Directory.CreateDirectory(pathDownloadFile);

            using (var sftp = new SftpClient(connectionInfo))
            {
                sftp.Connect();
                Logger.Info("Conectado al servidor SFTP.");
                var files = sftp.ListDirectory(remoteFilePath);

                foreach (var file in files)
                {
                    if (!file.IsDirectory && !file.Name.StartsWith("."))
                    {
                        var currentFile = file.Name;
                        var newFile = file.Name + ".pro";
                        if (!File.Exists(pathDownloadFile + currentFile + ".pro"))
                        {
                            using (var localFile = File.OpenWrite(pathDownloadFile + currentFile))
                            {

                                sftp.DownloadFile(remoteFilePath + currentFile, localFile);
                                Logger.Info("Archivo descargado correctamente.");
                                //sftp.RenameFile(remoteFilePath + currentFile, remoteFilePath + newFile);
                                //Logger.Info("Archivo renombrado correctamente.");
                            }
                        }
                        Logger.Info($"Archivo: {file.Name}");
                    }
                }
                sftp.Disconnect();
            }

            string[] excelFiles = Directory.GetFiles(pathDownloadFile, "*.xlsx");
            if (excelFiles.Length > 0)
            {
                foreach (var filePath in excelFiles)
                {
                    using (var workbook = new XLWorkbook(filePath))
                    {
                        List<Conciliacion> conciliacion = new List<Conciliacion>();
                        var worksheet = workbook.Worksheet(1);
                        var range = worksheet.RangeUsed();
                        if (range != null)
                        {
                            if (range.Columns().Count() == 40)
                            {

                                try
                                {
                                    int r = 0;
                                    foreach (var row in range.Rows())
                                    {
                                        Conciliacion con = new Conciliacion();
                                        if (r > 0)
                                        {
                                            int col = 0;
                                            try
                                            {
                                                foreach (var cell in row.Cells())
                                                {
                                                    Logger.Info($"Renglon: {r}, Valores: {cell.Value.ToString()}");
                                                    if (col == 0)
                                                        con.IdTransaction = Convert.ToInt64(cell.Value.ToString());
                                                    if (col == 1)
                                                        con.FechaHora = Convert.ToDateTime(cell.Value.ToString());
                                                    if (col == 2)
                                                        con.Tarjeta = cell.Value.ToString();
                                                    if (col == 3)
                                                        con.Monto = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 4)
                                                        con.Propina = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 5)
                                                        con.MontoTotal = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 6)
                                                        con.PorcentajeComision = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 7)
                                                        con.MontoComision = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 8)
                                                        con.IvaComision = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 9)
                                                        con.MSI = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 10)
                                                        con.SobreTasa = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 11)
                                                        con.MontoSobreTasa = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 12)
                                                        con.IvaSobreTasa = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 13)
                                                        con.FondoSeguridad = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 14)
                                                        con.FondoSeguridadAsignado = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 15)
                                                        con.FondoSeguridadRetenido = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 16)
                                                        con.MontoDepositar = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 17)
                                                        con.Autorizacion = cell.Value.ToString();
                                                    if (col == 18)
                                                        con.TipoTransaccion = cell.Value.ToString();
                                                    if (col == 19)
                                                        con.MarcaTarjeta = cell.Value.ToString();
                                                    if (col == 20)
                                                        con.TipoTarjeta = cell.Value.ToString();
                                                    if (col == 21)
                                                        con.TipoCaptura = cell.Value.ToString();
                                                    if (col == 22)
                                                        con.Banco = cell.Value.ToString();
                                                    if (col == 23)
                                                        con.Pais = cell.Value.ToString();
                                                    if (col == 24)
                                                        con.EtiquedaDispositivo = cell.Value.ToString();
                                                    if (col == 25)
                                                        con.ReferenciaTransaccion = cell.Value.ToString();
                                                    if (col == 26)
                                                        con.IdUsuario = cell.Value.ToString();
                                                    if (col == 27)
                                                        con.NombreComercial = cell.Value.ToString();
                                                    if (col == 28)
                                                        con.RazonSocial = cell.Value.ToString();
                                                    if (col == 29)
                                                        con.Afiliacion = cell.Value.ToString();
                                                    if (col == 30)
                                                        con.Lector = cell.Value.ToString();
                                                    if (col == 31)
                                                        con.FechaHoraTransConsi = Convert.ToDateTime(cell.Value.ToString());
                                                    if (col == 32)
                                                        con.Distribuidor = cell.Value.ToString();
                                                    if (col == 33)
                                                        con.Grupo = cell.Value.ToString();
                                                    if (col == 34)
                                                        con.FechaDeposito = Convert.ToDateTime(cell.Value.ToString());
                                                    if (col == 35)
                                                        con.Correo = cell.Value.ToString();
                                                    if (col == 36)
                                                        con.TipoPago = cell.Value.ToString();
                                                    if (col == 37)
                                                        con.Moneda = cell.Value.ToString();
                                                    if (col == 38)
                                                        con.ImporteComision = Convert.ToDecimal(cell.Value.ToString());
                                                    if (col == 39)
                                                        con.ImporteDepositado = Convert.ToDecimal(cell.Value.ToString());

                                                    col += 1;
                                                }
                                                conciliacion.Add(con);
                                            }
                                            catch (Exception ex)
                                            {
                                                Logger.Error($"Ocurrió un error al procesar el renglon {r}, error: {ex.Message}");
                                            }
                                        }
                                        r += 1;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Logger.Error($"Ocurrió un error al procesar su archivo {filePath}");
                                }
                            }
                            else
                            {
                                Logger.Error($"El archivo {filePath} no cumple con las 40 columnas que se necesitan para procesar la información.");
                            }
                        }
                        else
                        {
                            Logger.Error($"El achivo no tiene columnas {filePath}");
                        }

                        if (conciliacion.Count > 0)
                        {
                            // Registra en tabla temporal.
                            File.Move(filePath, filePath + ".pro");
                            Logger.Info($"Preparando DataTable temporal, para procesar masivamente: {conciliacion.Count}");
                            var jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(conciliacion);
                            DataTable dataTable = await PrepareCopyDataTableKushki(jsonString);
                            await Soltec.Orquestacion.DA.Orchestration.BulkCopyTableMySQL(dataTable, filePath);
                            Logger.Info($"Información procesada para el archivo {filePath}: {conciliacion.Count}");


                        }
                    }

                    //return true;
                }
            }
            else
            {
                Logger.Info("No se encontraron archivos a procesar.");
            }
            return true;
        }


        public static async Task<bool> ProcesaFacturasConceptosXML(string rutaFacturasXML)
        {
            var fileName = string.Empty;
            int contadorArchivos = 0;  // 🔥 Aquí controlamos el lote de 10

            if (Directory.Exists(rutaFacturasXML))
            {
                Logger.Info("Procesando los XMLs");

                var files = new DirectoryInfo(rutaFacturasXML)
                            .GetFiles()
                            .OrderBy(f => f.CreationTime)
                            .ToList();

                foreach (var file in files)
                {
                    if (file.Extension.ToUpper() == ".XML")
                    {
                        Logger.Info($"Procesando XML: {file.FullName}");

                        var result = ProcesaConceptos(file.FullName, file.Name);

                        if (result.Result)
                        {
                            File.Move(file.FullName, file.FullName + ".pro");
                            fileName = file.Name;

                            contadorArchivos++;    
                        }

                        if (contadorArchivos >= 10)
                        {
                            if (conceptos.Count > 0)
                            {
                                Logger.Info($"Preparando DataTable temporal: {conceptos.Count}");

                                var jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(conceptos);
                                DataTable dataTable = await PrepareCopyDataTable(jsonString);

                                Logger.Info($"Iniciando carga masiva con {conceptos.Count} conceptos...");
                                await Soltec.Orquestacion.DA.Orchestration.BulkCopyTableMySQLAsync(dataTable, fileName);

                                conceptos = new List<Conceptos>();
                            }
                            contadorArchivos = 0;
                        }
                    }
                }

                if (conceptos.Count > 0)
                {
                    Logger.Info($"Procesando lote final: {conceptos.Count}");

                    var jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(conceptos);
                    DataTable dataTable = await PrepareCopyDataTable(jsonString);

                    await Soltec.Orquestacion.DA.Orchestration.BulkCopyTableMySQLAsync(dataTable, fileName);

                    conceptos = new List<Conceptos>();
                }
            }
            else
            {
                Logger.Info("Ruta no encontrada para procesar los XMLs");
            }

            return true;
        }


        public static async Task<bool> ProcesaConceptos(string file, string fileName)
        {
            XDocument xml = XDocument.Load(file);
            XNamespace cfdi = "http://www.sat.gob.mx/cfd/4";
            XNamespace tfd = "http://www.sat.gob.mx/TimbreFiscalDigital";
            try
            {
                var comprobante = xml.Element(cfdi + "Comprobante");
                var folio = comprobante?.Attribute("Folio")?.Value;
                var fecha = comprobante?.Attribute("Fecha")?.Value;
                var _conceptos = xml.Descendants(cfdi + "Concepto");
                var complemento = comprobante?.Element(cfdi + "Complemento");
                var timbre = complemento?.Element(tfd + "TimbreFiscalDigital");
                var emisor = xml.Root.Element(cfdi + "Emisor");
                var uuid = string.Empty;

                if (emisor != null)
                {
                    string rfc = emisor.Attribute("Rfc")?.Value;
                    {
                        if (!string.IsNullOrEmpty(rfc) && rfc.ToUpper() == "FSI970908ML5")
                        {
                            if (timbre != null)
                                uuid = timbre.Attribute("UUID")?.Value;

                            foreach (var concepto in _conceptos)
                            {
                                Logger.Info($"Se va a subir la factura: {folio} - {Convert.ToDateTime(fecha)}");
                                var cantidad = concepto.Attribute("Cantidad")?.Value;
                                var valorUnitario = concepto.Attribute("ValorUnitario")?.Value;
                                var importe = concepto.Attribute("Importe")?.Value;
                                var descuento = concepto.Attribute("Descuento")?.Value;
                                conceptos.Add(new Conceptos()
                                {
                                    Folio = folio,
                                    Fecha = Convert.ToDateTime(fecha),
                                    ValorUnitario = string.IsNullOrEmpty(valorUnitario) ? 0 : Convert.ToDecimal(valorUnitario),
                                    ClaveProdServ = concepto.Attribute("ClaveProdServ")?.Value,
                                    NoIdentificacion = concepto.Attribute("NoIdentificacion")?.Value,
                                    Cantidad = string.IsNullOrEmpty(cantidad) ? 0 : Convert.ToDecimal(cantidad),
                                    ClaveUnidad = concepto.Attribute("ClaveUnidad")?.Value,
                                    Unidad = concepto.Attribute("Unidad")?.Value,
                                    Descripcion = concepto.Attribute("Descripcion")?.Value,
                                    Importe = string.IsNullOrEmpty(importe) ? 0 : Convert.ToDecimal(importe),
                                    Descuento = string.IsNullOrEmpty(descuento) ? 0 : Convert.ToDecimal(descuento),
                                    UUID = uuid,
                                    FileName = fileName
                                });
                            }
                        }
                        else
                        {
                            return false;
                        }
                    }
                    return true;
                }
                else
                    return false;
            }
            catch (Exception ex)
            {
                return false;
            }
        }


        public static async Task<bool> BackupBucketContaboAsync(string accessKey, string secretKey, string bucketName, string serviceBucketURL)
        {
            var result = true;
            var data = new List<DatosTicket>();
            try
            {
                data = (new Soltec.Orquestacion.DA.Orchestration().BackupBucketContaboAsync().Result).ToList();
                if (data.Count > 0)
                {
                    foreach (DatosTicket ticket in data)
                    {
                        string xmlUpload = returnXML(ticket.archivoxml);
                        var nombreArchivoXML = ticket.uuid + ".xml";

                        AmazonS3Config config = new AmazonS3Config();
                        config.ServiceURL = serviceBucketURL;
                        config.DisableHostPrefixInjection = true;
                        config.ForcePathStyle = true;

                        try
                        {
                            AmazonS3Client s3Client = new AmazonS3Client(accessKey, secretKey, config);
                            await CreateFolderAsync(s3Client, bucketName, $"{ticket.rfc}/{ticket.AnioCaptura}/{ticket.MesCaptura}/");
                            var bucketNameComplete = $"{ticket.rfc}/{ticket.AnioCaptura}/{ticket.MesCaptura}/";

                            var newMemoryStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(xmlUpload));
                            var putRequest = new PutObjectRequest
                            {
                                BucketName = bucketName,
                                Key = bucketNameComplete + nombreArchivoXML,
                                InputStream = newMemoryStream,
                                ContentType = "application/xml",
                                UseChunkEncoding = false
                            };

                            var response = s3Client.PutObjectAsync(putRequest).Result;
                            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                            {
                                Logger.Important($"Se procesó el ticket: {ticket.idDatosTicket} en el Bucket: {bucketNameComplete + nombreArchivoXML}");
                                var r = new Soltec.Orquestacion.DA.Orchestration().BackupBucketContaboUpdateAsync(ticket.idDatosTicket);
                                if (r.Result)
                                    Logger.Info($"El ticket: {ticket.idDatosTicket} se actualizó a procesado");
                                else
                                    Logger.Error($"El ticket: {ticket.idDatosTicket} NO se actualizó a procesado");
                            }
                            else
                            {
                                Logger.Error($"Ocurrió un error al procesar el ticket en el Bucket: {ticket.idDatosTicket}, respuesta del servicio. {response}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error(ex.Message);
                        }
                    }

                }
                return result;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return false;
            }
        }


        public static byte[] Decompress(byte[] data)
        {
            MemoryStream input = new MemoryStream(data);
            MemoryStream output = new MemoryStream();
            using (DeflateStream dstream = new DeflateStream(input, CompressionMode.Decompress))
            {
                dstream.CopyTo(output);
            }
            return output.ToArray();
        }
        public static async Task<bool> CreateFolderAsync(AmazonS3Client s3Client, string bucketName, string folderName)
        {
            try
            {
                // Crear un objeto vacío para simular la carpeta
                var putRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = folderName,
                    ContentBody = ""
                };
                PutObjectResponse response = s3Client.PutObjectAsync(putRequest).Result;

                Logger.Info($"Carpeta creada exitosamente: {folderName}");
            }
            catch (AmazonS3Exception s3Ex)
            {
                Logger.Error($"Error en Amazon S3: {s3Ex.Message}");
                return false;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error general: {ex.Message}");
                return false;
            }

            return true;
        }
        public static string returnXML(string xml)
        {

            string XMLBase64 = xml;
            string XML_zip = XMLBase64;
            byte[] bytes_XML_zip = System.Convert.FromBase64String(XML_zip);
            byte[] XML_arreglo1 = Decompress(bytes_XML_zip);
            string XMLstring = System.Text.Encoding.UTF8.GetString(XML_arreglo1, 0, XML_arreglo1.Length);

            //File.WriteAllText(@"c:\PDFS\" + uuid + ".xml", XMLstring);

            return XMLstring;
        }

        public static byte[] DecompressData(byte[] compressedData)
        {
            using (MemoryStream compressedStream = new MemoryStream(compressedData))
            using (GZipStream decompressionStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (MemoryStream decompressedStream = new MemoryStream())
            {
                decompressionStream.CopyTo(decompressedStream);
                return decompressedStream.ToArray();
            }
        }

        public static async Task<bool> BackupTicketsAsync()
        {
            var result = false;
            try
            {
                result = await new Soltec.Orquestacion.DA.Orchestration().BackupTicketsAsync();
                return result;
            }
            catch
            {
                return false;
            }
        }
        public static async Task<bool> OrquestacionDB()
        {
            string[] readScript = null;
            var result = false;
            try
            {
                result = await (new Soltec.Orquestacion.DA.Orchestration().OrquestacionDB());
                return result;
            }
            catch
            {
                return false;
            }
        }
        public static async Task<bool> ProcessFiles(string pathSourceFile, int idEmpresa, int valorInicial, int valorFinal)
        {
            return await Process(pathSourceFile, idEmpresa, valorInicial, valorFinal);
        }

        public static async Task<bool> RecibeTicket(string pathJSON, int peticion, string apiRecibeTicket)
        {
            string request = RegresaJSON(pathJSON).ToString();

            for (int x = 1; x <= peticion; x++)
            {
                try
                {
                    request = request.Replace("476327", x.ToString());
                    var httpClient = new HttpClient();
                    var content = new StringContent(request, Encoding.UTF8, "application/json");
                    httpClient.DefaultRequestHeaders.Clear();
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    httpClient.DefaultRequestHeaders.Add("apisimi", "de21acf9-bc05-479b-b61c-455de3fa0ae5");
                    Logger.Info($"Consumiendo servicio: {apiRecibeTicket}. No. {x}");

                    HttpResponseMessage responseMessage = httpClient.PostAsync(apiRecibeTicket, content).Result;
                    Logger.Info($"Response {apiRecibeTicket}: {responseMessage.StatusCode}. No. {x}");

                    // Valida si el estatus es OK
                    if (responseMessage.StatusCode == System.Net.HttpStatusCode.OK)
                        Logger.Info($"Respuesta exitosa.{responseMessage.StatusCode}. No. {x}");
                    else
                        Logger.Info($"Respuesta NO exitosa. {responseMessage.StatusCode}. No. {x}");
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }

            return true;

        }

        public static async Task<bool> ProcessFilesCatalogs(string pathSourceFile)
        {
            return await ProcessCatalog(pathSourceFile);
        }

        public static async Task<bool> LoadUpdateCatalogSQLServerAsync()
        {
            var result = await (new Soltec.Orquestacion.DA.Orchestration().LoadUpdateCatalogSQLServerAsync());
            return result;
        }

        static async Task<bool> Process(string pathSourceFile, int idEmpresa, int valorInicial, int valorFinal)
        {
            try
            {
                List<Files> filesList = new List<Files>();

                var sqlScripts = new Soltec.Orquestacion.DA.Orchestration().LoadSQLScripts().Result.Where(x => x.EsCatalogo == false).ToList();
                var serverMySQL = new Soltec.Orquestacion.DA.Orchestration().LoadOrchestratorServerMySQL(idEmpresa).Result.ToList().Skip(valorInicial).Take(valorFinal);

                DirectoryInfo dirs = new DirectoryInfo(pathSourceFile);
                var files = Directory.GetFiles(pathSourceFile, "*.zip", SearchOption.TopDirectoryOnly).Where(x => x.EndsWith(".zip"));

                if (serverMySQL == null && idEmpresa != 0)
                {
                    Logger.Error($"El proceso no puede ser ejecutado, la empresa {idEmpresa} no existe.");
                    return false;
                }

                if (Directory.Exists(pathSourceFile + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}"))
                    Directory.Delete(pathSourceFile + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}", true);

                if (idEmpresa != 0)
                {
                    foreach (string file in files)
                    {
                        var exist = serverMySQL.Where(x => x.ClaveSimi.ToUpper() == Path.GetFileNameWithoutExtension(file.Replace("Operativas_", "")).ToUpper()).FirstOrDefault();
                        if (exist != null)
                            filesList.Add(new Files() { FileName = Path.GetFileNameWithoutExtension(file) });
                    }
                }
                var processFiles = 0;
                if (filesList.Count > 0)
                {
                    foreach (var fileList in filesList)
                    {
                        try
                        {
                            dataSet = new DataSet();
                            string fileName = pathSourceFile + $"/" + fileList.FileName;
                            string directoryPath = Directory.GetParent(fileName)?.ToString();
                            var directoryChild = directoryPath + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}/{Path.GetFileNameWithoutExtension(fileName)}";

                            Logger.Info("Descomprimiendo zip: " + fileList.FileName + " en el directorio: " + directoryPath);
                            Directory.CreateDirectory(directoryChild.Replace("Operativas_", ""));
                            System.IO.Compression.ZipFile.ExtractToDirectory(fileName + ".zip", directoryChild.Replace("Operativas_", ""));
                            processFiles += 1;
                            try
                            {
                                if (File.Exists(fileName + ".pro"))
                                    File.Delete(fileName + ".pro");

                                File.Move(fileName + ".zip", fileName + ".pro");
                                //File.Delete(fileName + ".zip");
                            }
                            catch { }
                            Logger.Info($"Se descomprimió zip correctamente: {fileName} en el directorio: {directoryPath}");
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Ocurrió un error en el archivo .ZIP: {fileList.FileName} - {ex.Message}");
                        }
                    }
                    if (processFiles > 0)
                        Logger.Important($"Total de sucursales a procesar: {processFiles}");


                    if (Directory.Exists(pathSourceFile + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}"))
                    {
                        DirectoryInfo clientDirectory = new DirectoryInfo(pathSourceFile + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}/");
                        IEnumerable<FileInfo> fileList = clientDirectory.GetFiles("*.JSON", SearchOption.AllDirectories);

                        // Aplica para tablas operativas / movimientos con BASE DE DATOS asignada.
                        dataSet = new DataSet();
                        string tableName = string.Empty;
                        foreach (var fil in sqlScripts)
                        {
                            OrquestadorServidorMySQL orquestadorServidorMySQL = new OrquestadorServidorMySQL();
                            var filesResult = from file in fileList where file.Name.Contains(fil.Nombre) select file;
                            tableName = fil.Nombre;
                            DataTable dataTable = new DataTable(fil.Nombre);
                            var uuid = Guid.NewGuid().ToString();
                            foreach (var _file in filesResult)
                            {
                                if (_file.Name.ToUpper().Split('.')[0] == fil.Nombre.ToUpper())
                                {
                                    orquestadorServidorMySQL = serverMySQL.Where(x => x.ClaveSimi.ToUpper() == _file.Directory.Name.ToUpper()).FirstOrDefault();
                                    if (orquestadorServidorMySQL != null)
                                    {
                                        var idSucursal = orquestadorServidorMySQL.IdSucursal;
                                        Logger.Info($"Cargando tabla de paso para: {_file}");

                                        dataTable = await PrepareCopyTableMovtos(_file.FullName,
                                                                                 dataTable,
                                                                                 idSucursal,
                                                                                 orquestadorServidorMySQL.IdEmpresa,
                                                                                 uuid);

                                        //await Soltec.Orquestacion.DA.Orchestration.BulkCopyTable(dataTable,
                                        //														 orquestadorServidorMySQL,
                                        //														 uuid);
                                        fileCount += 1;
                                    }
                                }
                            }
                            //Logger.Important($"Nombre de la tabla {tableName}, total procesadas: {fileCount}");
                            //fileCount = 0;

                            //if (orquestadorServidorMySQL != null)
                            //{
                            if (dataTable.Rows.Count > 0)
                            {
                                await Soltec.Orquestacion.DA.Orchestration.BulkCopyTable(dataTable,
                                                                                         orquestadorServidorMySQL,
                                                                                         uuid);
                            }
                            //}
                        }

                        //Elimina carpeta temporal.
                        Directory.Delete(pathSourceFile + $"/Tmp_{idEmpresa}_{valorInicial}_{valorFinal}", true);

                    }
                    Logger.Important($"Termina carga de archivos; total de archivos procesados: {fileCount}");
                }
                else
                {
                    Logger.Warning("No se encontraron archivos a proces");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"No fue posible descomprimir los archivos zip, error: {ex.ToString()}");
                return true;
            }
            return true;
        }

        static async Task<bool> ProcessCatalog(string pathSourceFile)
        {
            try
            {
                List<Files> filesList = new List<Files>();
                var sqlScripts = new Soltec.Orquestacion.DA.Orchestration().LoadSQLScripts().Result.Where(x => x.EsCatalogo == true).ToList();
                var listSucursales = new Soltec.Orquestacion.DA.Orchestration().LoadSucursalesConfigCatalogos();
                DirectoryInfo dirs = new DirectoryInfo(pathSourceFile);
                var files = Directory.GetFiles(pathSourceFile, "*.zip", SearchOption.TopDirectoryOnly).Where(x => x.EndsWith(".zip") && x.Contains("Catalogos_"));

                if (Directory.Exists(pathSourceFile + $"\\Tmp"))
                    Directory.Delete(pathSourceFile + $"\\Tmp", true);

                foreach (string file in files)
                {
                    //var exist = listSucursales.Result.Where(x => x.ClaveSimi.Trim().ToUpper() == Path.GetFileNameWithoutExtension(file.Replace("Catalogos_", "").Trim().ToUpper()).ToUpper()).FirstOrDefault();
                    //if (exist != null)
                    filesList.Add(new Files() { FileName = Path.GetFileNameWithoutExtension(file).ToUpper() });
                }

                if (files.Count() > 0)
                {
                    foreach (var fileList in filesList)
                    {
                        try
                        {
                            dataSet = new DataSet();
                            string fileName = pathSourceFile + "\\" + fileList.FileName;
                            string directoryPath = Directory.GetParent(fileName)?.ToString();
                            var directoryChild = directoryPath + $"\\Tmp\\{Path.GetFileNameWithoutExtension(fileName)}";

                            Logger.Info("Descomprimiendo zip: " + fileList.FileName + " en el directorio: " + directoryPath);
                            Directory.CreateDirectory(directoryChild.Replace("CATALOGOS_", ""));
                            System.IO.Compression.ZipFile.ExtractToDirectory(fileName + ".zip", directoryChild.Replace("CATALOGOS_", ""));
                            try
                            {
                                File.Delete(fileName + ".zip");
                            }
                            catch { }
                            Logger.Info($"Se descomprimió zip correctamente: {fileName} en el directorio: {directoryPath}");
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Ocurrió un error en el archivo .ZIP: {fileList.FileName} - {ex.Message}");
                        }
                    }

                    if (Directory.Exists(pathSourceFile + $"\\Tmp"))
                    {
                        DirectoryInfo clientDirectory = new DirectoryInfo(pathSourceFile + $"\\Tmp\\");
                        IEnumerable<FileInfo> fileList = clientDirectory.GetFiles("*.JSON", SearchOption.AllDirectories);

                        // Aplica solo para catálogos
                        foreach (var fil in sqlScripts)
                        {
                            var filesResult = from file in fileList where file.Name.Contains(fil.Nombre) select file;

                            DataTable dataTable = new DataTable(fil.Nombre);
                            dataTable.Columns.Add("Sucursal", typeof(string));
                            foreach (var _file in filesResult)
                            {
                                var claveSucursalSimi = _file.DirectoryName.Split('.')[0].Split("\\")[_file.DirectoryName.Split('.')[0].Split("\\").Length - 1];
                                var serverOrquestador = listSucursales.Result.Where(x => x.ClaveSimi.ToUpper() == claveSucursalSimi.ToUpper()).FirstOrDefault();

                                if (_file.Name.ToUpper().Split('.')[0] == fil.Nombre.ToUpper())
                                {
                                    Logger.Info($"Cargando tabla de paso para el catálogo: {_file.FullName} de la sucursal: {_file.Directory.Name}");
                                    dataTable = await PrepareCopyTable(_file.FullName,
                                                                        dataTable,
                                                                        _file.Directory.Name);
                                    fileCount += 1;
                                }
                            }
                            if (dataTable.Rows.Count > 0)
                                await Soltec.Orquestacion.DA.Orchestration.BulkCopyTable(dataTable);
                            //await Soltec.Orquestacion.DA.Orchestration.BulkCopyTable(dataTable, serverOrquestador);
                        }


                        //Elimina carpeta temporal.
                        Directory.Delete(pathSourceFile + $"\\Tmp", true);

                    }
                    Logger.Important($"Termina carga de archivos; total de archivos procesados: {fileCount}");
                }
                else
                {
                    Logger.Warning("No se encontraron archivos a proces");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"No fue posible descomprimir los archivos zip, error: {ex.ToString()}");
                return true;
            }
            return true;
        }

        static async Task<DataTable> PrepareCopyTable(string file, DataTable dataTable, string sucursal)
        {
            DataTable dataTableCopy = new DataTable();
            try
            {
                using (var streamReader = new StreamReader(file))
                {
                    try
                    {
                        var tableName = Path.GetFileNameWithoutExtension(file);
                        var result = streamReader.ReadToEnd();
                        string dataSetTemplate = $"{{\"{tableName}\": [{result.Replace("[", "").Replace("]", "")}]}}";
                        DataSet _dataSet = JsonConvert.DeserializeObject<DataSet>(dataSetTemplate);

                        dataTableCopy = _dataSet.Tables[tableName];
                        dataTableCopy.Columns.Add("Sucursal", typeof(string));
                        foreach (DataRow row in dataTableCopy.Rows)
                            row["Sucursal"] = sucursal;

                        if (dataTable.Rows.Count <= 0)
                            dataTable = dataTableCopy.Copy();
                        else
                        {
                            DataRow[] rowsToCopy;
                            rowsToCopy = dataTableCopy.Select();
                            foreach (DataRow temp in rowsToCopy)
                                dataTable.ImportRow(temp);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                        return new DataTable();
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return new DataTable();
            }

            return dataTable;
        }


        static async Task<DataTable> PrepareCopyDataTable(string json)
        {
            var result = json;
            var tableName = "FacturasConceptosXML";
            DataTable dataTableCopy = new DataTable();
            dataTableCopy.TableName = "FacturasConceptosXML";
            try
            {
                string dataSetTemplate = $"{{\"{tableName}\": [{result.Replace("[", "").Replace("]", "")}]}}";
                DataSet _dataSet = JsonConvert.DeserializeObject<DataSet>(dataSetTemplate);

                dataTableCopy = _dataSet.Tables[tableName];
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return new DataTable();
            }

            return dataTableCopy;
        }

        static async Task<DataTable> PrepareCopyDataTableKushki(string json)
        {
            var result = json;
            var tableName = "ConciliacionKushki";
            DataTable dataTableCopy = new DataTable();
            dataTableCopy.TableName = "ConciliacionKushki";
            try
            {
                string dataSetTemplate = $"{{\"{tableName}\": [{result.Replace("[", "").Replace("]", "")}]}}";
                DataSet _dataSet = JsonConvert.DeserializeObject<DataSet>(dataSetTemplate);

                dataTableCopy = _dataSet.Tables[tableName];
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return new DataTable();
            }

            return dataTableCopy;
        }


        static async Task<DataTable> PrepareCopyTableMovtos(string file, DataTable dataTable, int idSucursal, int idEmpresa, string uuid)
        {
            DataTable dataTableCopy = new DataTable();
            try
            {
                using (var streamReader = new StreamReader(file))
                {
                    try
                    {
                        var tableName = Path.GetFileNameWithoutExtension(file);
                        var result = streamReader.ReadToEnd();
                        string dataSetTemplate = $"{{\"{tableName}\": [{result.Replace("[", "").Replace("]", "")}]}}";
                        DataSet _dataSet = JsonConvert.DeserializeObject<DataSet>(dataSetTemplate);

                        dataTableCopy = _dataSet.Tables[tableName];
                        dataTableCopy.Columns.Add("IdSucursal", typeof(int));
                        dataTableCopy.Columns.Add("IdEmpresa", typeof(string));
                        dataTableCopy.Columns.Add("UUID", typeof(string));

                        foreach (DataRow row in dataTableCopy.Rows)
                        {
                            row["IdSucursal"] = idSucursal;
                            row["IdEmpresa"] = idEmpresa;
                            row["UUID"] = uuid;
                        }

                        if (dataTable.Rows.Count <= 0)
                            dataTable = dataTableCopy.Copy();
                        else
                        {
                            DataRow[] rowsToCopy;
                            rowsToCopy = dataTableCopy.Select();
                            foreach (DataRow temp in rowsToCopy)
                                dataTable.ImportRow(temp);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                        return new DataTable();
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return new DataTable();
            }

            return dataTable;
        }



        static string RegresaJSON(string pathJSON)
        {
            string result = string.Empty;
            using (var streamReader = new StreamReader(pathJSON))
            {
                result = streamReader.ReadToEnd();
            }
            return result;
        }

        public static async Task<bool> ProcesaSQS(string accessKeySQS,
                                                  string secretKeySQS,
                                                  string regionSQS,
                                                  int idEmpresa)
        {
            var resultado = (await Soltec.Orquestacion.DA.Orchestration.CargaServidorSQL(idEmpresa));
            //foreach (var o in resultado)
            //{
                ProcesaSQS( accessKeySQS,
                            secretKeySQS,
                            regionSQS,
                            resultado.UrlSQS,
                            resultado);
            //}
            return true;
        }

        public static async Task<bool> ProcesaHistoricos(string[] urls)
        {
            var resultado = (await Soltec.Orquestacion.DA.Orchestration.CargaHistoricosRecibidos());
            foreach (var item in resultado)
            {
                if (string.IsNullOrEmpty(item.Clave))
                    Logger.Error("Sucursal no valida");

                //var urls = _configuration.GetSection("ApiSettings:Urls").Get<string[]>();

                if (urls == null || urls.Length == 0)
                    Logger.Error("No hay URLs de API configuradas.");

                byte[] fileBytes = null;

                // Intentar descargar desde la primera URL disponible
                foreach (var url in urls)
                {
                    try
                    {
                        //var client = _httpClientFactory.CreateClient();
                        using var client = new HttpClient();
                        client.BaseAddress = new Uri(url.EndsWith("/") ? url : url + "/");

                        var endpoint = new Uri(client.BaseAddress, $"venta/DescargarScriptZip?sucursal={item.Clave}");
                        var response = await client.GetAsync(endpoint);

                        if (response.IsSuccessStatusCode)
                        {
                            fileBytes = await response.Content.ReadAsByteArrayAsync();
                            break;
                        }
                    }
                    catch
                    {
                        continue;
                    }
                }

                if (fileBytes == null)
                    Logger.Error ("No se pudo descargar el archivo desde ninguna URL activa.");

                // --- Leer ZIP ---
                using var memoryStream = new MemoryStream(fileBytes);
                using var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read);

                TransmisionHistorico transmisionHistorico = null;
                SalesDataDto salesDataDto = null;

                foreach (var entry in archive.Entries)
                {
                    if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                        continue;

                    using var entryStream = entry.Open();
                    using var reader = new StreamReader(entryStream);
                    string jsonContent = await reader.ReadToEndAsync();

                    if (entry.Name == $"{item.Clave}_infoDB.json")
                    {
                        transmisionHistorico = JsonConvert.DeserializeObject<TransmisionHistorico>(jsonContent);
                    }
                    else if (entry.Name == $"{item.Clave}_data.json")
                    {
                        salesDataDto = JsonConvert.DeserializeObject<SalesDataDto>(jsonContent);
                    }
                }

                if (transmisionHistorico == null || salesDataDto == null)
                    Logger.Error("El ZIP no contiene los archivos JSON esperados.");

                // Procesar ambos archivos juntos
                var result = await Soltec.Orquestacion.DA.Orchestration.SincronizaHistoricos(transmisionHistorico, salesDataDto, item.Clave, item.Id);

            }

            return true;
        }


        public static async Task<bool> ProcesaSQS(string accessKeySQS,
                                                  string secretKeySQS,
                                                  string regionSQS,
                                                  string queueUrlSQS, 
                                                  OrquestadorServidorMySQL orquestador)
        {
            var result = true;
            var data = new List<DatosTicket>();

            try
            {
                var sqsClient = new AmazonSQSClient(
                        accessKeySQS,
                secretKeySQS,
                        RegionEndpoint.GetBySystemName(regionSQS)
                    );

                var receiveRequest = new ReceiveMessageRequest
                {
                    QueueUrl = queueUrlSQS,
                    MaxNumberOfMessages = 10,
                    WaitTimeSeconds = 20, // 20 segundos
                    VisibilityTimeout = 300, // 5 minutos para evitar traslape y esperar a que termine la carga en BD.
                    MessageAttributeNames = new List<string> { "All" }
                };

                //var purgeRequest = new PurgeQueueRequest
                //{
                //    QueueUrl = queueUrlSQS
                //};

                //sqsClient.PurgeQueueAsync(purgeRequest);

                while (true)
                {
                    var response = sqsClient.ReceiveMessageAsync(receiveRequest).Result;
                    string sucursal = string.Empty;
                    string formato = string.Empty;
                    string nombre = string.Empty;

                    if (response != null)
                    {
                        if (response.Messages != null)
                        {
                            if (response.Messages.Count == 0)
                                break; // cola vacía

                            foreach (var message in response.Messages)
                            {
                                Logger.Important($"MessageId: {message.MessageId}");
                                if (message.MessageAttributes.TryGetValue("Sucursal", out var sucursalAttr))
                                {
                                    sucursal = sucursalAttr.StringValue;
                                }
                                if (message.MessageAttributes.TryGetValue("Formato", out var formatoAttr))
                                {
                                    formato = formatoAttr.StringValue;
                                }

                                if (message.Body.Length <= 5)
                                    Console.Write(message.Body);

                                // Deserializar
                                var lista = CrearInstancia<List<InventarioSQS>>();
                                if (!string.IsNullOrEmpty(formato))
                                {
                                    if (formato == "JSON")
                                        lista = JsonConvert.DeserializeObject<List<InventarioSQS>>(message.Body);
                                    else
                                    {
                                        if (!string.IsNullOrEmpty(message.Body.Trim()))
                                        {
                                            lista = ParsePlano<InventarioSQS>(message.Body);
                                            if (lista.Count > 0)
                                            {
                                                var r = await Soltec.Orquestacion.DA.Orchestration.InsertarMasivoDatosInventarioAsync(lista, sucursal, orquestador);
                                                if (r)
                                                {
                                                    // Eliminar después de procesar
                                                    sqsClient.DeleteMessageAsync(queueUrlSQS, message.ReceiptHandle);
                                                }
                                                
                                            } else
                                                sqsClient.DeleteMessageAsync(queueUrlSQS, message.ReceiptHandle);
                                        } else
                                            sqsClient.DeleteMessageAsync(queueUrlSQS, message.ReceiptHandle);
                                    }
                                }
                            }
                        }
                    }
                }

                return result;
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                return false;
            }
        }

        public static T CrearInstancia<T>() where T : new()
        {
            // Declarar variable tipo T
            T instancia = new T();
            return instancia;
        }
        public static List<T> ParsePlano<T>(string plano) where T : new()
        {
            var resultado = new List<T>();

            if (string.IsNullOrWhiteSpace(plano))
                return resultado;

            var lineas = plano.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            var headers = lineas[0].Split('|'); // Encabezados

            foreach (var linea in lineas.Skip(1)) // Saltar encabezados
            {
                var valores = linea.Split('|');
                var obj = new T();

                for (int i = 0; i < headers.Length; i++)
                {
                    var prop = typeof(T).GetProperty(headers[i]);
                    if (prop != null && i < valores.Length)
                    {
                        object value = string.IsNullOrEmpty(valores[i]) ? null :
                                       Convert.ChangeType(valores[i], prop.PropertyType);
                        prop.SetValue(obj, value);
                    }
                }
                resultado.Add(obj);
            }

            return resultado;
        }

    }
}



public class ApiResponse
{
    public ApiResponse() => Success = true;

    public bool Success { get; set; } = true;

    public string Message { get; set; }

    public string Url { get; set; }

    public int? Count { get; set; }

    public ApiResponse(string itemString) : this() => ItemString = itemString;

    public ApiResponse(int itemInt) : this() => ItemId = itemInt;

    public string ItemString { get; set; }

    public int? ItemId { get; set; }

    public string DateRequest { get; set; } = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

    public ApiResponse(Exception ex)
    {
        Success = false;
        Message = ex.Message;
    }
}

public class ApiResponse<T> : ApiResponse
{
    public ApiResponse() { }

    public ApiResponse(T result) => Result = result;

    public ApiResponse(List<T> result)
    {
        List = result;
        Count = List.Count;
    }

    public List<T> List { get; set; }

    public T Result { get; set; }

}

public class MonitorDeApps
{
    public int IdMonitor { get; set; }

    public string Ruta { get; set; }

    public string NombreEXE { get; set; }

    public string NombreProceso { get; set; }
    public bool Activo { get; set; }

}

