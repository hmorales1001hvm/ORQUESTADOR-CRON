using Azure;
using Dapper;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Mysqlx.Session;
using MySqlX.XDevAPI.Common;
using Soltec.Common.Logger;
using Soltec.Orquestacion.DA.Entities;
using Soltec.Orquestacion.Entidades;
using Soltec.Orquestacion.Entidades.DTOs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using System.Xml.Linq;
using static OnDemandDTO;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Soltec.Orquestacion.DA
{
    public class Orchestration
    {
        static string conectionString = Settings1.Default.ConectionString;
        static string conectionStringMaqueta = Settings1.Default.ConectionStringMaqueta;
        static string conectionStringFacturacion = Settings1.Default.ConectionStringFacturacion;
        static string conextionStringFacturaRealOrquestador = Settings1.Default.ConectionStringFacturaRealOrquestador;
        static string conectionSIMIPET = Settings1.Default.ConectionSIMIPET;

        static int batchSize = 1500;


        public async Task<bool> OrquestacionDB()
        {
            try
            {
                bool result = true;
                string path = string.Empty;
                //string stringConexion = string.Empty;

                string stringConexion =
            "Data Source=62.146.228.210;" +
            "Initial Catalog=FRA_RENA;" +
            "User ID=orquestador;" +
            "Password=S0lt3cC0nsultor3s##++;" +
            "Integrated Security=False; Persist Security Info=False;" +
            "Trusted_Connection=False;TrustServerCertificate=True;";

                try
                {
                    Logger.Info("Creando DataTable con 10,000 registros...");

                    DataTable dt = new DataTable();
                    dt.Columns.Add("Id", typeof(int));
                    dt.Columns.Add("Nombre", typeof(string));

                    for (int i = 1; i <= 10000; i++)
                        dt.Rows.Add(i, $"Registro {i}");

                    Logger.Info("Conectando al servidor...");

                    using (SqlConnection con = new SqlConnection(stringConexion))
                    {
                        con.Open();

                        Logger.Info("Conexión abierta. Iniciando BulkCopy...");

                        var bulk = new SqlBulkCopy(con)
                        {
                            DestinationTableName = "dbo.PruebaBulkCopy",
                            BatchSize = 2000,
                            BulkCopyTimeout = 60
                        };

                        var inicio = DateTime.Now;

                        bulk.WriteToServer(dt);

                        var fin = DateTime.Now;
                        var ms = (fin - inicio).TotalMilliseconds;

                        Logger.Info("BulkCopy ejecutado correctamente.");
                        Logger.Info($"Tiempo total: {ms} ms");
                    }
                }
                catch (Exception ex)
                {
                    Logger.Info("Ocurrió un error durante la prueba:");
                    Logger.Info(ex.Message);
                }

                var servers = LoadServersDB();
                foreach (var s in servers.Result)
                {
                    try
                    {
                        stringConexion = $"Data Source=62.146.228.210;" +
                                                $"Initial Catalog=FRA_RENA;" +
                                                $"user id=orquestador;" +
                                                $"Password=S0lt3cC0nsultor3s##++;" +
                                                $"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;";

                        using (SqlConnection connection = new SqlConnection(stringConexion))
                        {
                            connection.Open();
                            SqlCommand sqlCommand = new SqlCommand();

                            sqlCommand.Connection = connection;
                            sqlCommand.CommandType = CommandType.Text;
                            SqlDataAdapter da = new SqlDataAdapter("SELECT * FROM SYSDATABASES;", connection);
                            DataTable dataTable = new DataTable();
                            da.Fill(dataTable);

                            bool exist = false;
                            foreach (DataRow dr in dataTable.Rows)
                            {
                                if (dr["name"].ToString().ToUpper() == s.DatabaseName.ToUpper())
                                {
                                    exist = true;
                                    break;
                                }
                            }

                            if (!exist)
                            {
                                if (string.IsNullOrEmpty(path))
                                    path = returnPathBCK();

                                sqlCommand = new SqlCommand();
                                sqlCommand.Connection = connection;
                                sqlCommand.ResetCommandTimeout();
                                sqlCommand.CommandTimeout = 2000;
                                sqlCommand.CommandText = @"	RESTORE DATABASE " + s.DatabaseName.ToUpper() +
                                                          " FROM DISK = '" + path + "'" +
                                                          " WITH MOVE 'FRA_MAQUETA' TO '/var/opt/mssql/data/" + s.DatabaseName.ToUpper() + ".mdf'," +
                                                          " MOVE 'FRA_MAQUETA_Log' TO 	'/var/opt/mssql/data/" + s.DatabaseName.ToUpper() + "_log.ldf', RECOVERY, REPLACE;";

                                sqlCommand.ExecuteNonQuery();
                                Logger.Info($"Base de datos {s.DatabaseName} creada exitosamente.");
                            }
                            else
                            {
                                // Carga script para homologar DBs
                                DataTable dataTableDB = new DataTable();
                                using (var cnx = new MySqlConnection(conectionString))
                                {
                                    cnx.Open();
                                    MySqlCommand _sqlCommand = new MySqlCommand("SELECT * FROM soltec2_orquestador_db;", cnx);
                                    _sqlCommand.Connection = cnx;
                                    _sqlCommand.CommandType = CommandType.Text;
                                    MySqlDataAdapter daDB = new MySqlDataAdapter(_sqlCommand);
                                    daDB.Fill(dataTableDB);
                                    cnx.Close();
                                }
                                foreach (DataRow dr in dataTableDB.Rows)
                                {
                                    if (!string.IsNullOrEmpty(dr["ScriptDB"].ToString()))
                                    {
                                        try
                                        {
                                            var sql = $"USE {s.DatabaseName}";
                                            sqlCommand = new SqlCommand();
                                            sqlCommand.Connection = connection;
                                            sqlCommand.CommandText = sql;
                                            sqlCommand.ExecuteNonQuery();

                                            sqlCommand.CommandText = $"{dr["ScriptDB"].ToString()}";
                                            sqlCommand.ExecuteNonQuery();
                                        }
                                        catch (Exception ex)
                                        {
                                            Logger.Error(ex.Message);
                                        }
                                    }
                                }
                            }
                            connection.Close();
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                    }
                }
                Logger.Important("Finaliza gestión de BD.");
                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return false;
            }
        }

        string returnPathBCK()
        {
            string stringConexion = conectionStringMaqueta;
            string path = string.Empty;
            using (SqlConnection connection = new SqlConnection(stringConexion))
            {
                SqlCommand sqlcmd = new SqlCommand();
                path = @"/var/opt/mssql/data/MAQUETA" + System.DateTime.Now.ToString("HHmmss") + ".BAK";
                try
                {
                    connection.Open();
                    sqlcmd = new SqlCommand("backup database FRA_MAQUETA to disk='" + path + "'", connection);
                    sqlcmd.ExecuteNonQuery();
                    Logger.Info("Backup exitoso de la BD");
                    connection.Close();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }
            return path;
        }

        public static async Task<bool> BulkCopyTable(DataTable dataTable)
        {

            try
            {
                bool result = true;
                string strCNX = Settings1.Default.ConectionStringAdministrativo;
                using (var connection = new SqlConnection(strCNX))
                {
                    connection.Open();
                    try
                    {
                        var bulkCopy = new SqlBulkCopy(connection);

                        bulkCopy.DestinationTableName = "tmp_" + dataTable.TableName.ToLower();
                        var cols = GetSqlColumnMapping(dataTable).ToList();
                        foreach (var col in cols)
                        {
                            bulkCopy.ColumnMappings.Add(col);
                        }
                        bulkCopy.BatchSize = 100000;
                        bulkCopy.WriteToServer(dataTable);
                        Logger.Important($"Se copió correctamente en la tabla: {"tmp_" + dataTable.TableName.ToLower()}");

                        var parameter = new DynamicParameters();
                        parameter.Add("@Catalogo", dataTable.TableName.ToUpper(), DbType.String);
                        connection.Execute("usp_Orquestador_ReplicaCatalogos", parameter, commandType: CommandType.StoredProcedure, commandTimeout: 5000);
                        connection.Close();
                        Logger.Important($"Replica de información correcta");
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                        connection.Close();
                    }

                }

                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return false;
            }
        }

        public static async Task<bool> BulkCopyTableMySQLAsync(DataTable dataTable, string file)
        {
            bool result = true;
            string strCNX = Settings1.Default.ConectionStringFacturaRealOrquestador;

            using (var connection = new MySqlConnection(strCNX))
            {
                await connection.OpenAsync();
                string tmpTable = "tmp_" + dataTable.TableName;

                try
                {
                    var bulkCopy = new MySqlBulkCopy(connection)
                    {
                        DestinationTableName = tmpTable
                    };

                    bulkCopy.ColumnMappings.AddRange(GetMySqlColumnMapping(dataTable));

                    // 🚀 Insert masivo
                    await bulkCopy.WriteToServerAsync(dataTable);

                    Logger.Important($"Carga masiva: {tmpTable}");

                    var parameter = new DynamicParameters();
                    parameter.Add("@File", file, DbType.String);

                    // 🚀 Ejecuta SPs
                    await connection.ExecuteAsync("usp_OrquestadorFacturasConceptosXML",
                                                   parameter, commandType: CommandType.StoredProcedure);

                    await connection.ExecuteAsync("usp_RegistraEnFacturacionDetalle",
                                                   parameter, commandType: CommandType.StoredProcedure);

                    Logger.Important("Procedimientos ejecutados correctamente.");
                }
                catch (Exception ex)
                {
                    Logger.Error($"Error en BulkCopy: {ex.Message}");

                    try
                    {
                        // 🚨 TRUNCAR TABLA CUANDO FALLA
                        string truncateSql = $"TRUNCATE TABLE {tmpTable}";
                        await connection.ExecuteAsync(truncateSql);
                        Logger.Important($"Tabla temporal limpiada: {tmpTable}");
                    }
                    catch (Exception ex2)
                    {
                        Logger.Error($"Error al truncar {tmpTable}: {ex2.Message}");
                    }

                    result = false;
                }
                finally
                {
                    if (connection.State != ConnectionState.Closed)
                        await connection.CloseAsync();
                }
            }

            return result;
        }


        public static async Task<bool> BulkCopyTableMySQL(DataTable dataTable, string file)
        {
            try
            {
                bool result = true;
                string strCNX = Settings1.Default.ConectionStringFacturaRealOrquestador;
                using (var connection = new MySqlConnection(strCNX))
                {
                    connection.Open();
                    try
                    {
                        var bulkCopy = new MySqlBulkCopy(connection);

                        bulkCopy.DestinationTableName = "tmp_" + dataTable.TableName;
                        bulkCopy.ColumnMappings.AddRange(GetMySqlColumnMapping(dataTable));
                        MySqlBulkCopyColumnMapping mySqlBulkCopyColumnMapping = new MySqlBulkCopyColumnMapping();

                        bulkCopy.WriteToServer(dataTable);
                        Logger.Important($"Carga masiva ConciliacionKushki: {"tmp_" + dataTable.TableName.ToLower()}");

                        var parameter = new DynamicParameters();
                        parameter.Add("@File", file, DbType.String);
                        connection.Execute("usp_OrquestadorConciliacionKushki", parameter, commandType: CommandType.StoredProcedure, commandTimeout: 5000);
                        connection.Close();
                        Logger.Important($"Se ha terminado la carga del SP: usp_OrquestadorConciliacionKushki");
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                        connection.Close();
                    }

                }

                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return false;
            }
        }


        public static async Task<bool> BulkCopyTable(DataTable dataTable, OrquestadorServidorMySQL serverMySQL, string UUID)
        {
            bool result = true;
            string stringConexion = string.Empty;
            try
            {
                stringConexion = $"Data Source={serverMySQL.HostName};" +
                                    $"Initial Catalog={serverMySQL.DatabaseName};" +
                                    $"user id={serverMySQL.UserName};" +
                                    $"Password={serverMySQL.Password};" +
                                    $"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;";

                using (var connection = new SqlConnection(stringConexion))
                {
                    connection.Open();

                    try
                    {
                        var bulkCopy = new SqlBulkCopy(connection);

                        bulkCopy.DestinationTableName = "tmp_" + dataTable.TableName.ToLower();
                        var cols = GetSqlColumnMapping(dataTable).ToList();
                        foreach (var col in cols)
                        {
                            bulkCopy.ColumnMappings.Add(col);
                        }
                        bulkCopy.BatchSize = 100000;
                        bulkCopy.WriteToServer(dataTable);
                        Logger.Important($"Se replicó correctamente en: {"tmp_" + dataTable.TableName.ToLower()} - BD: {serverMySQL.DatabaseName}");

                        Logger.Important($"Iniciando carga en tablas operativas; {serverMySQL.Empresa}");
                        var parameter = new DynamicParameters();
                        parameter.Add("@UUID", UUID, DbType.String);
                        connection.Execute("usp_Orquestador_ReplicaOperativas", parameter, commandType: CommandType.StoredProcedure, commandTimeout: 1500);

                        Logger.Important($"Carga correcta en la tabla: {dataTable.TableName.ToLower()}, total de registros {dataTable.Rows.Count}. {serverMySQL.Empresa}");

                        connection.Close();
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(ex.Message);
                        connection.Close();
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                result = false;
            }

            return result;
        }

        public static async Task<bool> BulkCopyTableGenerica(DataSet dataSet)
        {
            bool result = true;
            string stringConexion = Settings1.Default.ConectionString;
            try
            {
                using (var connection = new MySqlConnector.MySqlConnection(stringConexion))
                {
                    connection.Open();
                    foreach (DataTable dataTable in dataSet.Tables)
                    {
                        try
                        {
                            var bulkCopy = new MySqlBulkCopy(connection);

                            bulkCopy.DestinationTableName = "tmp_" + dataTable.TableName.ToLower();
                            bulkCopy.ColumnMappings.AddRange(GetMySqlColumnMapping(dataTable));
                            MySqlBulkCopyColumnMapping mySqlBulkCopyColumnMapping = new MySqlBulkCopyColumnMapping();

                            bulkCopy.WriteToServer(dataTable);
                            Logger.Important($"Se replicó correctamente en: {"tmp_" + dataTable.TableName.ToLower()}");
                        }
                        catch (Exception ex)
                        {
                            Logger.Error(ex.Message);
                        }
                    }
                    connection.Close();
                }

                using (var connection = new MySqlConnector.MySqlConnection(stringConexion))
                {
                    Logger.Important($"Iniciando carga en tablas operativas genéricas.");
                    connection.Open();
                    connection.Execute("usp_Orquestador_ReplicaOperativas", commandType: CommandType.StoredProcedure, commandTimeout: 2000);
                    connection.Close();
                    Logger.Important($"Termina carga en tablas operativas genéricas.");
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                result = false;
            }

            return result;
        }

        private static List<MySqlBulkCopyColumnMapping> GetMySqlColumnMapping(DataTable dataTable)
        {
            List<MySqlBulkCopyColumnMapping> colMappings = new List<MySqlBulkCopyColumnMapping>();
            int i = 0;
            foreach (DataColumn col in dataTable.Columns)
            {
                colMappings.Add(new MySqlBulkCopyColumnMapping(i, col.ColumnName));

                i++;
            }
            return colMappings;
        }

        private static List<SqlBulkCopyColumnMapping> GetSqlColumnMapping(DataTable dataTable)
        {
            var colMappings = new List<SqlBulkCopyColumnMapping>();
            int i = 0;
            foreach (DataColumn col in dataTable.Columns)
            {
                colMappings.Add(new SqlBulkCopyColumnMapping(i, col.ColumnName));

                i++;
            }
            return colMappings;
        }


        public async Task<List<SPOS_SQLScripts>> LoadSQLScripts()
        {
            var queryScripts = @" SELECT    SS.IdSqlScript,
	                                        SS.SQLScript ,
	                                        SS.Nombre,
	                                        SS.Tipo,
	                                        SS.Condicion,
	                                        SS.ValorIncrementoDecremento,
	                                        SS.EsAPI,
                                            SS.Activo,
                                            SS.Descripcion,
											SS.EsCatalogo
                                  FROM spos_sqlscripts SS WHERE SS.Activo = 1";
            var sqlScripts = new List<SPOS_SQLScripts>();

            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    sqlScripts = connection.Query<SPOS_SQLScripts>(queryScripts, commandType: CommandType.Text, commandTimeout: 420).ToList();
                    connection.Close();
                }

                return sqlScripts;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                throw;
            }
        }


        public async Task<bool> LoadUpdateCatalog()
        {
            var result = true;
            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    using (MySqlConnector.MySqlCommand command = new MySqlConnector.MySqlCommand("usp_Orquestador_ActualizaConcentradoCatalogos", connection))
                    {
                        using (MySqlConnector.MySqlDataAdapter da = new MySqlConnector.MySqlDataAdapter(command))
                        {

                            da.SelectCommand.CommandType = CommandType.StoredProcedure;
                            DataTable dt = new DataTable();
                            da.Fill(dt);
                            Logger.Important($"Se encontraron: {dt.Rows.Count}, códigos de productos repetidos.");
                            foreach (DataRow dr in dt.Rows)
                            {
                                Logger.Info($"Código: {dr["codigoProducto"].ToString()} - {dr["Total"].ToString()}");
                            }
                        }
                    }
                    connection.Close();
                    result = true;
                }

            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                result = false;
                throw;
            }

            return result;
        }

        public async Task<bool> LoadUpdateCatalogSQLServerAsync()
        {
            var result = true;
            try
            {
                using (var connection = new SqlConnection(conectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand("usp_Orquestador_ActualizaConcentradoCatalogos", connection))
                    {
                        using (SqlDataAdapter da = new SqlDataAdapter(command))
                        {

                            da.SelectCommand.CommandType = CommandType.StoredProcedure;
                            DataTable dt = new DataTable();
                            da.Fill(dt);
                            Logger.Important($"Se encontraron: {dt.Rows.Count}, códigos de productos repetidos.");
                            foreach (DataRow dr in dt.Rows)
                            {
                                Logger.Info($"Código: {dr["codigoProducto"].ToString()} - {dr["Total"].ToString()}");
                            }
                        }
                    }
                    connection.Close();
                    result = true;
                }

            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                result = false;
                throw;
            }

            return result;
        }

        public async Task<List<OrquestadorServidorMySQL>> LoadOrchestratorServerMySQL(int idEmpresa)
        {
            var result = new List<OrquestadorServidorMySQL>();

            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    DynamicParameters parameter = new DynamicParameters();
                    parameter.Add("pIdEmpresa", idEmpresa, DbType.Int64);
                    result = connection.Query<OrquestadorServidorMySQL>("usp_Orquestador_ServidorMySQL", parameter, commandType: CommandType.StoredProcedure, commandTimeout: 420).ToList();
                    connection.Close();
                }

                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                throw;
            }
        }

        public async Task<List<Sucursales>> LoadSucursales()
        {
            var result = new List<Sucursales>();

            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    result = connection.Query<Sucursales>("SELECT S.claveSimi, S.IdSucursal FROM sucursal s WHERE s.Estatus ='A'", commandType: CommandType.Text, commandTimeout: 420).ToList();
                    connection.Close();
                }

                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                throw;
            }
        }

        public async Task<List<Sucursales>> LoadSucursalesConfigCatalogos()
        {
            var result = new List<Sucursales>();

            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    result = connection.Query<Sucursales>(@"SELECT S.claveSimi, S.IdSucursal FROM sucursal s 
															INNER JOIN soltec2_orquestador_config_sucursales_catalogos B ON s.claveSimi = B.ClaveSucursal
															WHERE s.Estatus ='A' AND B.Activo = 1 ", commandType: CommandType.Text, commandTimeout: 420).ToList();
                    connection.Close();
                }

                return result;
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                throw;
            }
        }

        public async static Task<List<OrquestadorServidorMySQL>> LoadServersDB()
        {
            List<OrquestadorServidorMySQL> serversDB = new List<OrquestadorServidorMySQL>();
            var queryScripts = @"	SELECT  B.IdEmpresa,
											B.HostName,
											B.UserName,
											B.Password,
											B.DatabaseName,
											B.Activo,
											A.Port,
											A.DBReference,
											A.UserNameReference,
											A.PasswordReference,
                                            B.HostNamePublic, 
                                            D.ClaveSimi, 
                                            CAST(puerto AS VARCHAR(50)) Puerto
									FROM soltec2_orquestador_servidormysql 					A 
									INNER JOIN soltec2_orquestador_servidormysql_detalle	B ON A.IdOrquestadorServidorMySql = B.IdOrquestadorServidorMySql 
                                    INNER JOIN catempresa C ON B.IdEmpresa = C.idEmpresa    
                                    INNER JOIN sucursal D ON C.idEmpresa = D.idEmpresa 
                                    WHERE B.Activo = 1";
            try
            {
                using (var connection = new MySqlConnection(conextionStringFacturaRealOrquestador))
                {
                    connection.Open();
                    serversDB = connection.Query<OrquestadorServidorMySQL>(queryScripts, commandType: CommandType.Text, commandTimeout: 420).ToList();
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
            }

            return serversDB;
        }

        public async Task<bool> BackupTicketsAsync()
        {
            var result = false;
            var dataTickets = new List<DatosTicket>();
            try
            {
                using (var connection = new MySqlConnection(conectionString))
                {
                    connection.Open();
                    dataTickets = (await connection.QueryAsync<DatosTicket>("usp_BackupTickets", commandType: CommandType.StoredProcedure, commandTimeout: 420)).ToList();
                    Logger.Important($"Se registrarán ({dataTickets.Count})");
                    connection.Close();
                    InsertTickets(dataTickets);
                    result = true;
                }

            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                throw;
            }

            return result;
        }



        public string InsertTickets(List<DatosTicket> tickets)
        {
            string respuesta = "";
            using (var conexion = new SqlConnection(conectionStringFacturacion))
            {
                try
                {
                    conexion.Open();
                    foreach (var t in tickets)
                    {
                        var parameter = new DynamicParameters();
                        parameter.Add("@idDatosTicket", t.idDatosTicket, DbType.Int32);
                        parameter.Add("@sucursal", t.sucursal, DbType.String);
                        parameter.Add("@codigoBarras", t.codigoBarras, DbType.String);
                        parameter.Add("@total", t.total, DbType.Decimal);
                        parameter.Add("@rfc", t.rfc, DbType.String);
                        parameter.Add("@razonSocial", t.razonSocial, DbType.String);
                        parameter.Add("@cp", t.cp, DbType.String);
                        parameter.Add("@idRegimenFiscal", t.idRegimenFiscal, DbType.String);
                        parameter.Add("@claveCfdi", t.claveCfdi, DbType.String);
                        parameter.Add("@formaPago", t.formaPago, DbType.String);
                        parameter.Add("@correo", t.correo, DbType.String);
                        parameter.Add("@fechaCaptura", t.fechaCaptura, DbType.DateTime);
                        parameter.Add("@uuid", t.uuid, DbType.String);
                        parameter.Add("@archivoxml", t.archivoxml, DbType.String);
                        parameter.Add("@Estatus", t.Estatus, DbType.String);
                        parameter.Add("@empresa_id", t.empresa_id, DbType.String);
                        parameter.Add("@TotalTicket", t.TotalTicket, DbType.Decimal);
                        parameter.Add("@TotalFacturado", t.TotalFacturado, DbType.Decimal);
                        parameter.Add("@NotaCredito", t.NotaCredito, DbType.String);
                        parameter.Add("@NotaProcesada", t.NotaProcesada, DbType.Int32);
                        parameter.Add("@RFCEmisor", t.RFCEmisor, DbType.String);
                        parameter.Add("@Prueba", t.Prueba, DbType.Int32);
                        parameter.Add("@uuidNota", t.uuidNota, DbType.String);
                        parameter.Add("@convertido", t.convertido, DbType.Int32);
                        parameter.Add("@NotaOK", t.NotaOK, DbType.String);
                        parameter.Add("@FacturaCancelada", t.FacturaCancelada, DbType.Int32);
                        parameter.Add("@NotaCancelada", t.NotaCancelada, DbType.Int32);
                        parameter.Add("@simifactura", t.simifactura, DbType.Int32);
                        parameter.Add("@TicketConsolidado", t.TicketConsolidado, DbType.String);
                        parameter.Add("@IvaFactura", t.IvaFactura, DbType.Decimal);
                        parameter.Add("@IvaNotaCredito", t.IvaNotaCredito, DbType.Decimal);
                        parameter.Add("@DescuentoFactura", t.DescuentoFactura, DbType.Decimal);
                        parameter.Add("@DescuentoNota", t.DescuentoNota, DbType.Decimal);
                        parameter.Add("@fechaNCR", t.fechaNCR, DbType.DateTime);
                        parameter.Add("@uuidrelacionado", t.uuidrelacionado, DbType.String);
                        parameter.Add("@Comentarios", t.Comentarios, DbType.String);
                        parameter.Add("@fechaTicket", t.fechaTicket, DbType.DateTime);
                        parameter.Add("@sucursalNCR", t.sucursalNCR, DbType.String);
                        parameter.Add("@vCfdi", t.vCfdi, DbType.String);
                        parameter.Add("@errorDescripcion", t.errorDescripcion, DbType.String);
                        parameter.Add("@pais", t.pais, DbType.String);
                        parameter.Add("@registroTributario", t.registroTributario, DbType.String);
                        parameter.Add("@ErrorDescripcionNCR", t.ErrorDescripcionNCR, DbType.String);
                        parameter.Add("@Multifran", t.Multifran, DbType.Int32);
                        parameter.Add("@codigoError", t.codigoError, DbType.String);
                        parameter.Add("@MultifranNCR", t.MultifranNCR, DbType.Int32);
                        parameter.Add("@fechaCreacion", t.fechaCreacion, DbType.DateTime);
                        parameter.Add("@fechaModificacion", t.fechaModificacion, DbType.DateTime);
                        parameter.Add("@ticketEnCentral", t.ticketEnCentral, DbType.Int32);
                        parameter.Add("@FechaTimbrado", t.FechaTimbrado, DbType.DateTime);
                        parameter.Add("@codigoErrorNCR", t.codigoErrorNCR, DbType.String);
                        parameter.Add("@TotalNCR", t.TotalNCR, DbType.Decimal);
                        parameter.Add("@UUIDFacturaGlobal", t.UUIDFacturaGlobal, DbType.String);
                        parameter.Add("@sistemaTimbra", t.sistemaTimbra, DbType.String);
                        parameter.Add("@ImpuestoTasa", t.ImpuestoTasa, DbType.Decimal);
                        parameter.Add("@ImpuestoBase", t.ImpuestoBase, DbType.Decimal);
                        parameter.Add("@serieNCR", t.serieNCR, DbType.String);
                        try
                        {
                            conexion.Execute("usp_InsertaTicket", parameter, commandType: CommandType.StoredProcedure, commandTimeout: 420);
                            Logger.Info($"Se registró el ticket: {t.idDatosTicket}");
                        }
                        catch (Exception ex) { Logger.Error(ex.Message); }
                    }
                    conexion.Close();
                    Logger.Important($"Carga terminada de tickets.");
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }
            return respuesta;
        }


        public async Task<List<DatosTicket>> BackupBucketContaboAsync()
        {
            var dataTickets = new List<DatosTicket>();
            string sql = @"	SELECT	idDatosTicket,
									rfc, 
									uuid, 
									YEAR(fechaCaptura) AnioCaptura, 
									MONTH(fechaCaptura) MesCaptura,
									archivoxml
							FROM datos_ticket 
							WHERE CONVERT(VARCHAR,fechaCreacion,112) = CONVERT(VARCHAR,GETDATE(),112) AND Procesado = 0";
            using (var conexion = new SqlConnection(conectionStringFacturacion))
            {
                try
                {
                    conexion.Open();
                    dataTickets = (await conexion.QueryAsync<DatosTicket>(sql, commandType: CommandType.Text, commandTimeout: 420)).ToList();
                    conexion.Close();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }
            return dataTickets;
        }


        public async Task<bool> BackupBucketContaboUpdateAsync(int idTicket)
        {
            var dataTickets = new List<DatosTicket>();

            string sql = @"UPDATE datos_ticket SET Procesado = 1 WHERE idDatosTicket=" + idTicket;
            using (var conexion = new SqlConnection(conectionStringFacturacion))
            {
                try
                {
                    conexion.Open();
                    conexion.Execute(sql, commandType: CommandType.Text, commandTimeout: 420);
                    conexion.Close();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                    return false;
                }
            }
            return true;
        }

        public async Task<bool> ProcesaDatosVentaDepositos(string arrayEmpresas)
        {
            var data = new List<OrquestacionClientes_Depositos>();
            string sql = $"	SELECT * FROM soltec2_OrquestacionClientes_Depositos WHERE Activo = 1 AND IdEmpresa IN({arrayEmpresas.Replace("[", "").Replace("]", "")});";
            using (var conexion = new MySqlConnection(conectionString))
            {
                try
                {
                    conexion.Open();
                    data = (await conexion.QueryAsync<OrquestacionClientes_Depositos>(sql, commandType: CommandType.Text, commandTimeout: 420)).ToList();
                    conexion.Close();
                    Logger.Important($"Carga de clientes con depositos a procesar: {data.Count}.");
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }

            if (data.Count > 0)
            {
                foreach (var d in data)
                {
                    Logger.Info($"Iniciando conexión para el cliente {d.Dominio}, bd: {d.DB} ");
                    var cnx = $"Data Source={d.Dominio};Initial Catalog={d.DB}; " +
                                $"user id={d.Usuario}; " +
                                $"Password={d.Password}; " +
                                $"Integrated Security=False; " +
                                $"Persist Security Info=False;" +
                                $"Trusted_Connection=False;" +
                                $"TrustServerCertificate=True;";
                    using (var conexion = new SqlConnection(cnx))
                    {
                        try
                        {

                            sql = @"select SC_CVE, SC_NOMBRE, E.RFC, convert(varchar(10), fecha, 120) as fecha, VENTA_NETA
									from 
									(
											Select VTA.id_sucursal, S.SC_CVE, S.SC_NOMBRE, Id_EmpresaGrupo, fecha, sum(CAB_TOTAL) - sum(DEVOLUCION) as VENTA_NETA
											from (
													select vc.id_sucursal, vc.vt_fecha as fecha, SUM(VT_TOTAL) as CAB_TOTAL , 0 as DET_TOTAL, 0 AS FACTURAS,  0 as NCR,0  as DEVOLUCION, 0  as DEPOSITO, 0 AS PRODS_INVENT, 0 as AjusteMas, 0 AS AjusteMin
													from dbo.opeVtaCabecera vc with (nolock) " +
                                                    $" where CONVERT(VARCHAR,vt_fecha,112) between CONVERT(VARCHAR,{d.FechaInicial},112) and CONVERT(VARCHAR,{d.FechaFinal},112) " +
                                                    @" group by vc.id_sucursal, vc.vt_fecha
													UNION
													select id_sucursal,dv_fecha as fecha, 0 as CAB_TOTAL, 0 DET_TOTAL, 0 as Facturas ,  0 as NCR, sum(dv_total) as DEVOLUCION , 0  as DEPOSITO, 0 AS PRODS_INVENT, 0 as AjusteMas, 0 AS AjusteMin
													from opeDevCabecera " +
                                                    $" where convert(char(8),dv_fecha,112) between  CONVERT(VARCHAR,{d.FechaInicial},112) and CONVERT(VARCHAR,{d.FechaFinal},112) " +
                                                    @" group by id_sucursal,dv_fecha
											) Vta
											INNER JOIN CATSUCURSALES S ON S.ID_SUCURSAL = VTA.ID_SUCURSAL " +
                                            $" INNER JOIN (select distinct id_sucursal from opeHexistencias where CONVERT(VARCHAR,e_fecha,112) = CONVERT(VARCHAR,{d.FechaFinal},112)) INV on Vta.id_sucursal = INV.id_sucursal " +
                                            @" group by VTA.id_sucursal, S.SC_CVE, S.SC_NOMBRE, Id_EmpresaGrupo, fecha
									) Totales
									inner join catEmpresasGrupo E on e.Id_EmpresaGrupo = Totales.Id_EmpresaGrupo";

                            Logger.Important($"Ejecutando query: {sql}");
                            var dataVentas = new List<VentaDepositos>();
                            conexion.Open();
                            dataVentas = (await conexion.QueryAsync<VentaDepositos>(sql, commandType: CommandType.Text, commandTimeout: 420)).ToList();
                            conexion.Close();
                            Logger.Important($"Depósitos a procesar: {dataVentas.Count} para el cliente: {d.IdEmpresa}, base de datos. {d.DB}");

                            if (dataVentas.Count > 0)
                            {
                                using (var connection = new MySqlConnection(conectionString))
                                {
                                    connection.Open();
                                    // Realizar bulkCopy
                                    DataTable dataTable = await LoadDataTable(dataVentas);
                                    try
                                    {
                                        Logger.Important($"Ejecutando BulkCopy: tmp_Venta");
                                        var bulkCopy = new MySqlBulkCopy(connection);

                                        bulkCopy.DestinationTableName = "tmp_Venta";
                                        bulkCopy.ColumnMappings.AddRange(GetMySqlColumnMapping(dataTable));
                                        MySqlBulkCopyColumnMapping mySqlBulkCopyColumnMapping = new MySqlBulkCopyColumnMapping();

                                        bulkCopy.WriteToServer(dataTable);
                                        Logger.Important($"Se replicó correctamente en: tmp_Venta");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error(ex.Message);
                                    }

                                    try
                                    {
                                        Logger.Important($"Cargando datos en tabla Operativa de Ventas");
                                        connection.Execute("usp_RegistraVentasDepositos", commandType: CommandType.StoredProcedure, commandTimeout: 420);
                                        Logger.Important($"Termina carga en tabla Operativa de Ventas");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error(ex.Message);
                                    }
                                    //                           foreach (var v in dataVentas)
                                    //{
                                    //	Logger.Important($"Registrando la venta para: {v.SC_CVE}, fecha: {v.Fecha}, Venta: {v.VENTA_NETA}");

                                    //                               DynamicParameters parameter = new DynamicParameters();
                                    //	parameter.Add("p_CVE", v.SC_CVE, DbType.String);
                                    //	parameter.Add("p_FechaVenta", v.Fecha, DbType.Date);
                                    //	parameter.Add("p_Venta", v.VENTA_NETA, DbType.Decimal);
                                    //	parameter.Add("p_idUsuario", 1, DbType.Int32);
                                    //	parameter.Add("p_llave", $"{v.SC_CVE}_{v.Fecha.ToString("yyyyMMdd")}" , DbType.String);

                                    //	Logger.Important($"Registro correcto de la venta para: {v.SC_CVE}, fecha: {v.Fecha}, Venta: {v.VENTA_NETA}");
                                    //}
                                    connection.Close();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error(ex.Message);
                        }
                    }
                }
            }
            else
            {
                Logger.Warning($"No se encontraron registros a procesar.");
            }
            return true;
        }

        public async Task<DataTable> LoadDataTable(List<VentaDepositos> ventasDepositos)
        {
            DataTable table = new DataTable();
            table.Columns.Add("CVE", typeof(string));
            table.Columns.Add("FechaVenta", typeof(DateTime));
            table.Columns.Add("Venta", typeof(decimal));
            table.Columns.Add("idUsuario", typeof(int));
            table.Columns.Add("Llave", typeof(string));
            foreach (var v in ventasDepositos)
                table.Rows.Add(v.SC_CVE, v.Fecha, v.VENTA_NETA, 1, $"{v.SC_CVE}_{v.Fecha.ToString("yyyyMMdd")}");
            return table;
        }
        public static async Task<DataTable> LoadDataTableProd(List<ProductosMultifran> productosMultifran)
        {
            DataTable table = new DataTable();
            table.Columns.Add("NoIdentificacion", typeof(string));
            table.Columns.Add("Descripcion", typeof(string));
            table.Columns.Add("Compra", typeof(decimal));
            table.Columns.Add("Venta", typeof(decimal));
            table.Columns.Add("EsInvent", typeof(bool));
            table.Columns.Add("EsKit", typeof(bool));
            foreach (var v in productosMultifran)
                table.Rows.Add(v.NoIdentificacion, v.Descripcion, v.Compra, v.Venta, v.EsInvent, v.EsKit);
            return table;


            //    public string NoIdentificacion { get; set; }
            //public string Descripcion { get; set; }
            //public decimal Compra { get; set; }
            //public decimal Venta { get; set; }
            //public bool EsInvent { get; set; }
            //public bool EsKit { get; set; }
        }



        public static async Task<bool> RegistraProductosFaltantes()
        {
            var data = new List<OrquestacionClientes_Depositos>();
            string sql = $"	SELECT * FROM soltec2_OrquestacionClientes_Depositos WHERE Activo = 1;";
            using (var conexion = new MySqlConnection(conectionString))
            {
                try
                {
                    conexion.Open();
                    data = (conexion.Query<OrquestacionClientes_Depositos>(sql, commandType: CommandType.Text, commandTimeout: 420)).ToList();
                    conexion.Close();
                    Logger.Important($"Carga de catálogo de Productos: {data.Count}.");
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }

            if (data.Count > 0)
            {
                foreach (var d in data)
                {
                    Logger.Info($"Iniciando conexión para el cliente {d.Dominio}, bd: {d.DB} ");
                    var cnx = $"Data Source={d.Dominio};Initial Catalog={d.DB}; " +
                                $"user id={d.Usuario}; " +
                                $"Password={d.Password}; " +
                                $"Integrated Security=False; " +
                                $"Persist Security Info=False;" +
                                $"Trusted_Connection=False;" +
                                $"TrustServerCertificate=True;";
                    using (var conexion = new SqlConnection(cnx))
                    {
                        try
                        {
                            sql = @"SELECT p_codigo NoIdentificacion, p_nombre Descripcion, 
									p_prevta Venta, p_invent EsInvent, 
									p_kit EsKit, p_precom Compra 
									FROM catProductos WHERE PATINDEX('%[^a-zA-Z0-9-/]%', p_codigo) = 0";

                            Logger.Important($"Ejecutando query: {sql}");
                            var dataProd = new List<ProductosMultifran>();
                            conexion.Open();
                            dataProd = (conexion.Query<ProductosMultifran>(sql, commandType: CommandType.Text, commandTimeout: 420)).ToList();
                            conexion.Close();
                            Logger.Important($"Productos a procesar: {dataProd.Count} para el cliente: {d.IdEmpresa}, base de datos. {d.DB}");

                            if (dataProd.Count > 0)
                            {
                                using (var connection = new MySqlConnection(Settings1.Default.ConectionStringFacturaRealOrquestador))
                                {
                                    connection.Open();
                                    // Realizar bulkCopy
                                    DataTable dataTable = await LoadDataTableProd(dataProd);
                                    try
                                    {
                                        Logger.Important($"Ejecutando BulkCopy: tmp_ProductosMultiFran");
                                        var bulkCopy = new MySqlBulkCopy(connection);

                                        bulkCopy.DestinationTableName = "tmp_ProductosMultiFran";
                                        bulkCopy.ColumnMappings.AddRange(GetMySqlColumnMapping(dataTable));
                                        MySqlBulkCopyColumnMapping mySqlBulkCopyColumnMapping = new MySqlBulkCopyColumnMapping();

                                        bulkCopy.WriteToServer(dataTable);
                                        Logger.Important($"Se replicó correctamente en: tmp_ProductosMultiFran");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error(ex.Message);
                                    }

                                    try
                                    {
                                        Logger.Important($"Cargando datos en tabla catProductos de tmp_ProductosMultiFran");

                                        DynamicParameters param = new DynamicParameters();
                                        param.Add("pDominio", d.Dominio, DbType.String);
                                        param.Add("pDB", d.DB, DbType.String);

                                        connection.Execute("usp_RegistraProductosMultiFran", param, commandType: CommandType.StoredProcedure, commandTimeout: 420);
                                        Logger.Important($"Termina carga en tabla catProductos de tmp_ProductosMultiFran");
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.Error(ex.Message);
                                    }

                                    connection.Close();
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error(ex.Message);
                        }
                    }
                }
            }
            else
            {
                Logger.Warning($"No se encontraron registros a procesar.");
            }
            return true;
        }


        public static async Task<bool> InsertarMasivoDatosInventarioAsync(List<InventarioSQS> tickets, string sucursal, OrquestadorServidorMySQL orquestador)
        {
            bool success = true;
            try
            {
                string cadenaConexion = string.Empty;
                //var resultado = (await CargaServidorSQL(sucursal));
                if (orquestador != null)
                {
                    cadenaConexion = $"Data Source={orquestador.HostName};" +
                                    $"Initial Catalog={orquestador.DatabaseName};" +
                                    $"user id={orquestador.UserName};" +
                                    $"Password={orquestador.Password};" +
                                    $"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;Connect Timeout=60;";
                    using (var connection = new SqlConnection(cadenaConexion))
                    {
                        connection.Open();

                        var table = new DataTable();
                        table.Columns.Add("ClaveSimi", typeof(string));
                        table.Columns.Add("FechaOperacion", typeof(DateTime));
                        table.Columns.Add("Id_Producto", typeof(long));
                        table.Columns.Add("ExistenciaInicial", typeof(long));
                        table.Columns.Add("Entradas", typeof(long));
                        table.Columns.Add("Salidas", typeof(long));
                        table.Columns.Add("ExistenciaFinal", typeof(long));

                        foreach (var item in tickets)
                        {
                            DateTime fecha = DateTime.ParseExact(
                              item.FechaOperacion,
                              "dd/MM/yyyy hh:mm:ss tt",
                              new CultureInfo("es-MX")
                          );
                            table.Rows.Add(sucursal, fecha, item.Id_Producto, item.ExistenciaInicial, item.Entradas, item.Salidas, item.ExistenciaFinal);
                        }


                        using var createTempCmd = new SqlCommand(@"CREATE TABLE #TempInventarios (
																									ClaveSimi VARCHAR(50),
																									FechaOperacion DATETIME,
																									Id_Producto BIGINT,
																									ExistenciaInicial INT,
																									Entradas INT,
																									Salidas INT,
																									ExistenciaFinal INT
																								);", connection);

                        createTempCmd.ExecuteNonQuery();
                        using (var bulkCopy = new SqlBulkCopy(connection)
                        {
                            DestinationTableName = "#TempInventarios",
                            BulkCopyTimeout = 2600
                        })
                        {
                            bulkCopy.WriteToServer(table);
                        }

                        string mergeSql = @"
                                    MERGE INTO Inventarios AS target
                                    USING #TempInventarios AS source
                                    ON target.ClaveSimi = source.ClaveSimi AND target.Id_Producto = source.Id_Producto
									WHEN MATCHED THEN 
                                        UPDATE SET 
											FechaOperacion = source.FechaOperacion,
                                            ExistenciaInicial = source.ExistenciaInicial,
                                            Entradas = source.Entradas,
                                            Salidas = source.Salidas,
											ExistenciaFinal = source.ExistenciaFinal
                                    WHEN NOT MATCHED THEN
                                        INSERT (ClaveSimi, 
												FechaOperacion, 
												Id_Producto, 
												ExistenciaInicial, 
												Entradas,
												Salidas,
												ExistenciaFinal)
                                        VALUES (source.ClaveSimi, 
												source.FechaOperacion, 
												source.Id_Producto, 
												source.ExistenciaInicial,
                                                source.Entradas, 
												source.Salidas,
												source.ExistenciaFinal);";
                        using var mergeCmd = new SqlCommand(mergeSql, connection)
                        {
                            CommandTimeout = 2600
                        };
                        var total = mergeCmd.ExecuteNonQuery();

                        connection.Close();
                        Logger.Important($"SQS Procesado correctamente para la sucursal: {sucursal}");
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                success = false;
            }

            return success;
        }

        public static async Task<OrquestadorServidorMySQL> CargaServidorSQL(int idEmpresa)
        {
            const string queryScripts = @"	SELECT HostName,
												   UserName,
												   B.Password,
												   DatabaseName,
												   B.IdEmpresa,
												   IFNULL(B.UrlSQS,'') AS UrlSQS
											FROM soltec2_orquestador_servidormysql A
											INNER JOIN soltec2_orquestador_servidormysql_detalle B ON A.IdOrquestadorServidorMySql = B.IdOrquestadorServidorMySql
											WHERE B.Activo = 1 AND B.IdEmpresa = @idEmpresa;";
            try
            {
                await using var connection = new MySqlConnection(conextionStringFacturaRealOrquestador);
                connection.Open();

                var server = connection.QuerySingleOrDefault<OrquestadorServidorMySQL>(
                    queryScripts,
                    new { idEmpresa },
                    commandType: CommandType.Text,
                    commandTimeout: 420);

                connection.Close();
                return server ?? new OrquestadorServidorMySQL();
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
                return new OrquestadorServidorMySQL();
            }
        }



        public static async Task<List<ModelTransmisiones>> CargaHistoricosRecibidos()
        {
            //ProbarConexion();

            try
            {
                using var connection = new MySqlConnection(conextionStringFacturaRealOrquestador);
                var parameters = new DynamicParameters();
                parameters.Add("@pIdEmpresa", 0, DbType.Int32);
                parameters.Add("@pIdUsuario", 0, DbType.Int32);
                parameters.Add("@pHistorico", 0, DbType.Int32);
                parameters.Add("@pEstatus", "RECIBIDO", DbType.String);

                await connection.OpenAsync();
                Logger.Info("Conexión abierta correctamente");

                var data = await connection.QueryAsync<ModelTransmisiones>(
                    "usp_PortalCargaTransmisionesNormalHistoricos",
                    parameters,
                    commandType: CommandType.StoredProcedure,
                    commandTimeout: 420
                );

                return data.ToList();
            }
            catch (Exception ex)
            {
                Logger.Info($"Error al abrir la conexión: {ex.Message}");
                throw;
            }
        }

        public static async Task<List<ModelTransmisiones>> CargaHistoricosRecibidosOnDemand()
        {
            //ProbarConexion();

            try
            {
                using var connection = new MySqlConnection(conextionStringFacturaRealOrquestador);
                var parameters = new DynamicParameters();
                parameters.Add("@pIdEmpresa", 0, DbType.Int32);
                parameters.Add("@pIdUsuario", 0, DbType.Int32);
                parameters.Add("@pHistorico", 0, DbType.Int32);
                parameters.Add("@pEstatus", "RECIBIDO", DbType.String);

                await connection.OpenAsync();
                Logger.Info("Conexión abierta correctamente");

                var data = await connection.QueryAsync<ModelTransmisiones>(
                    "usp_PortalCargaTransmisionesHistoricosOnDemand",
                    parameters,
                    commandType: CommandType.StoredProcedure,
                    commandTimeout: 420
                );

                return data.ToList();
            }
            catch (Exception ex)
            {
                Logger.Info($"Error al abrir la conexión: {ex.Message}");
                throw;
            }
        }

        //public static async Task ProbarConexion()
        //{
        //    string s = "Server=soltec2-sqlinst01.public.63c8f9674a3e.database.windows.net,3342;Persist Security Info=False;Database=FRA_FARRAM;User Id=orquestador;Password=7e+ROsWU!Hi1#phu;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=60;";

        //    try
        //    {
        //        using var connection = new SqlConnection(s);
        //        await connection.OpenAsync();

        //        var ventas = await connection.QueryAsync("SELECT TOP 10 * FROM [dbo].[Ventas]");

        //        Console.WriteLine($"Registros obtenidos: {ventas.Count()}");
        //        Console.WriteLine("Conexión exitosa");
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error: {ex.Message}");
        //    }
        //}

        public static async Task<List<ModelTransmisiones>> CargaHistoricosRecibidosSIMIPET()
        {
            try
            {
                using var connection = new MySqlConnection(conectionSIMIPET);
                var parameters = new DynamicParameters();
                parameters.Add("@pIdEmpresa", 0, DbType.Int32);
                parameters.Add("@pIdUsuario", 0, DbType.Int32);
                parameters.Add("@pHistorico", 2, DbType.Int32);
                parameters.Add("@pEstatus", "RECIBIDO", DbType.String);

                await connection.OpenAsync();
                Logger.Info("Conexión abierta correctamente");

                var data = await connection.QueryAsync<ModelTransmisiones>(
                    "usp_PortalCargaTransmisionesHistoricos",
                    parameters,
                    commandType: CommandType.StoredProcedure,
                    commandTimeout: 420
                );

                return data.ToList();
            }
            catch (Exception ex)
            {
                Logger.Info($"Error al abrir la conexión: {ex.Message}");
                throw;
            }
        }

        public static async Task<bool> ActualizarEstatus(int id, string sucursal, string estatusNuevo, string estatusEsperado)
        {
            using var conn = new MySqlConnection(conextionStringFacturaRealOrquestador);
            await conn.OpenAsync();

            const string query = @" UPDATE soltec2_Historicos
                                    SET Estatus = @EstatusNuevo
                                    WHERE IdHistorico = @IdHistorico
                                      AND ClaveSimi = @ClaveSimi
                                      AND Estatus = @EstatusEsperado";

            using var cmd = new MySqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@EstatusNuevo", estatusNuevo);
            cmd.Parameters.AddWithValue("@EstatusEsperado", estatusEsperado);
            cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
            cmd.Parameters.AddWithValue("@IdHistorico", id);

            return await cmd.ExecuteNonQueryAsync() == 1;
        }

        public static async Task<bool> SincronizaHistoricosOnDemand(OnDemandDTO dto, string sucursal, OrquestadorServidorMySQL orquestador)
        {
            using var connection = new SqlConnection(orquestador.ConnectionString);

            try
            {
                Logger.Info($"Iniciando OnDemand | Sucursal {sucursal}");

                await connection.OpenAsync();

                using var cmd = new SqlCommand("dbo.usp_OrquestadorOnDemand", connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 5000
                };

                // =========================
                // TVP: Producto
                // =========================
                if (dto.producto?.Count > 0)
                {
                    var table = await DataTableAsync(dto.producto);
                    cmd.Parameters.AddWithValue("@Producto", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@Producto"].TypeName = "dbo.TvpProducto";
                }

                // =========================
                // TVP: ProductoRecomendado
                // =========================
                if (dto.productoRecomendado?.Count > 0)
                {
                    var table = await DataTableAsync(dto.productoRecomendado);
                    cmd.Parameters.AddWithValue("@ProductoRecomendado", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@ProductoRecomendado"].TypeName = "dbo.TvpProductoRecomendado";
                }

                // =========================
                // TVP: ProductoCombo
                // =========================
                if (dto.productoCombo?.Count > 0)
                {
                    var table = await DataTableAsync(dto.productoCombo);
                    cmd.Parameters.AddWithValue("@ProductoCombo", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@ProductoCombo"].TypeName = "dbo.TvpProductoCombo";
                }

                // =========================
                // TVP: SPOSInventarios
                // =========================
                var minSqlDate = new DateTime(1753, 1, 1);

                var inv = dto.inventarios?
                    .Where(x =>
                        !string.IsNullOrWhiteSpace(x.Codigo) &&
                        x.FechaOperacion > minSqlDate
                    )
                    .ToList();

                if (inv?.Any() == true)
                {
                    inv.ForEach(x =>
                    {
                        x.IdEmpresa = orquestador.IdEmpresa;
                        x.FechaOperacion = x.FechaOperacion.Date; // opcional
                    });

                    var table = await DataTableAsync(inv);

                    cmd.Parameters.AddWithValue("@Inventarios", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@Inventarios"].TypeName = "dbo.TvpInventarios";
                }

                // =========================
                // TVP: SPOS Facturas
                // =========================
                if (dto.facturas?.Count > 0)
                {
                    dto.facturas.ForEach(x => x.IdEmpresa = orquestador.IdEmpresa);
                    var table = await DataTableAsync(dto.facturas);
                    cmd.Parameters.AddWithValue("@SPOSFacturas", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@SPOSFacturas"].TypeName = "dbo.TvpSPOSFacturas";
                }

                await cmd.ExecuteNonQueryAsync();

                //using (var conn = new MySqlConnection(conextionStringFacturaRealOrquestador))
                //{
                //    await conn.OpenAsync();
                //    string query = @"UPDATE soltec2_Historicos SET Estatus = 'PROCESADO', Activo = 0, FechaProcesado=SYSDATE() WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                //    using (var cmd2 = new MySqlCommand(query, conn))
                //    {
                //        cmd2.Parameters.AddWithValue("@ClaveSimi", sucursal);
                //        cmd2.Parameters.AddWithValue("@IdHistorico", id);
                //        await cmd.ExecuteNonQueryAsync();
                //    }


                //    await conn.CloseAsync();
                //}

                Logger.Info("OnDemand V2 finalizado correctamente");
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error en OnDemand V2: {ex.Message}");
                return false;
            }
        }


        public static async Task<bool> SincronizaHistoricos(ConectDB data, SalesDataDto salesDataDto, string sucursal, int id, OrquestadorServidorMySQL orquestadorServidorMySQL)
        {
            using var connection = new SqlConnection(orquestadorServidorMySQL.ConnectionString);
            var nombreProceso = string.Empty;

            try
            {
                Logger.Info($"Procesando la sucursal {sucursal}");


                await connection.OpenAsync();
                Logger.Info($"Iniciando SincronizaSetDeTransmisionesSQLServer - Sucursal: {sucursal}");
                int idEmpresa = orquestadorServidorMySQL.IdEmpresa;

                var dto = salesDataDto;
                // ============================================================
                // Preparar datos válidos y asignar IdEmpresa
                // ============================================================
                var ventasValidas = dto.Ventas?.Where(v => v.Id_Venta != null).ToList();
                ventasValidas?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasProductos = dto.VentasProductos?.Where(v => v.Id_Venta.HasValue && v.Cantidad.HasValue).ToList();
                ventasProductos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImpuestos = dto.VentasImpuestos?.Where(v => v.Id_Venta != null).ToList();
                ventasImpuestos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImpuestosDetalle = dto.VentasImpuestosDetalle?.Where(v => v.Id_Venta != null).ToList();
                ventasImpuestosDetalle?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasDesgloceTotales = dto.VentasDesgloceTotales?.Where(v => v.Id_Venta != null).ToList();
                ventasDesgloceTotales?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImportesProductos = dto.VentasImportesProductos?.Where(v => v.Id_Venta != null).ToList();
                ventasImportesProductos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var inventarioCosto = dto.InventarioCosto?
                                                            .Where(i => !string.IsNullOrWhiteSpace(i.ClaveSimi)
                                                                     && !string.IsNullOrWhiteSpace(i.Codigo)
                                                                     && i.FechaFactura != default)
                                                            .ToList();


                var sposInventario = dto.SPOSInventario?.ToList();

                var sposFacturas = dto.SPOSFacturas?.ToList();

                var ventasVendedorCuotasConSucursal = dto.VentasVendedorCuotas
                    ?.Select(v => new VentasVendedorCuotasDto
                    {
                        ClaveSimi = data.Sucursal,
                        Fecha = v.Fecha,
                        IdVendedor = v.IdVendedor,
                        Nombre = v.Nombre,
                        ImporteVenta = v.ImporteVenta,
                        Transaccionesventa = v.Transaccionesventa,
                        PorcVenta = v.PorcVenta,
                        ImporteNaturistas = v.ImporteNaturistas,
                        PorcNaturistas = v.PorcNaturistas,
                        ImporteNocturno = v.ImporteNocturno,
                        MontoDescuento = v.MontoDescuento,
                        Menudeos = v.Menudeos,
                        MontoIva = v.MontoIva,
                        IdEmpresa = idEmpresa
                    }).ToList();

                // ============================================================
                // Ejecutar SP maestro solo si hay al menos 1 tabla con datos
                // ============================================================
                bool hayDatos = (ventasValidas?.Count > 0) ||
                                (ventasProductos?.Count > 0) ||
                                (ventasImpuestos?.Count > 0) ||
                                (ventasImpuestosDetalle?.Count > 0) ||
                                (ventasDesgloceTotales?.Count > 0) ||
                                (ventasImportesProductos?.Count > 0) ||
                                (ventasVendedorCuotasConSucursal?.Count > 0) ||
                                (inventarioCosto?.Count > 0) ||
                                (sposInventario?.Count > 0) ||
                                (sposFacturas?.Count > 0);

                if (hayDatos)
                {
                    var sw = Stopwatch.StartNew();

                    using var cmd = new SqlCommand("dbo.usp_OrquestadorVentasMaestro", connection)
                    {
                        CommandType = CommandType.StoredProcedure,
                        CommandTimeout = 1200
                    };

                    if (ventasValidas?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasValidas);
                        cmd.Parameters.AddWithValue("@Ventas", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@Ventas"].TypeName = "dbo.TvpVentas";
                    }

                    if (ventasProductos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasProductos);
                        cmd.Parameters.AddWithValue("@VentasProductos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasProductos"].TypeName = "dbo.TvpVentasProductos";
                    }

                    if (ventasImpuestos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImpuestos);
                        cmd.Parameters.AddWithValue("@VentasImpuestos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImpuestos"].TypeName = "dbo.TvpVentasImpuestos";
                    }

                    if (ventasImpuestosDetalle?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImpuestosDetalle);
                        cmd.Parameters.AddWithValue("@VentasImpuestosDetalle", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImpuestosDetalle"].TypeName = "dbo.TvpVentasImpuestosDetalle";
                    }

                    if (ventasDesgloceTotales?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasDesgloceTotales);
                        cmd.Parameters.AddWithValue("@VentasDesgloseTotales", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasDesgloseTotales"].TypeName = "dbo.TvpVentasDesgloseTotales";
                    }

                    if (ventasImportesProductos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImportesProductos);
                        cmd.Parameters.AddWithValue("@VentasImportesProductos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImportesProductos"].TypeName = "dbo.TvpVentasImportesProductos";
                    }

                    if (ventasVendedorCuotasConSucursal?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasVendedorCuotasConSucursal);
                        cmd.Parameters.AddWithValue("@VentasVendedorCuotas", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasVendedorCuotas"].TypeName = "dbo.TvpVentasVendedorCuotas";
                    }

                    await cmd.ExecuteNonQueryAsync();

                }
                connection.Dispose();
                using (var conn = new MySqlConnection(conextionStringFacturaRealOrquestador))
                {
                    await conn.OpenAsync();
                    string query = @"UPDATE soltec2_Historicos SET Estatus = 'PROCESADO', Activo = 0, FechaProcesado=SYSDATE() WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                    using (var cmd = new MySqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
                        cmd.Parameters.AddWithValue("@IdHistorico", id);
                        await cmd.ExecuteNonQueryAsync();
                    }


                    await conn.CloseAsync();
                }

                Logger.Info($"Sincronización completada correctamente.");
                return true;
            }
            catch (Exception ex)
            {
                using (var conn = new MySqlConnection(conextionStringFacturaRealOrquestador))
                {
                    await conn.OpenAsync();
                    string query = @"UPDATE soltec2_Historicos SET Estatus = 'RECIBIDO' WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                    using (var cmd = new MySqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
                        cmd.Parameters.AddWithValue("@IdHistorico", id);
                        await cmd.ExecuteNonQueryAsync();
                    }


                    await conn.CloseAsync();
                }
                Logger.Error($"Ocurrió un error: {ex.Message}");
                return false;
            }
        }

        private static async Task<DataTable> DataTableAsync<T>(IEnumerable<T> data)
        {
            if (data == null) return null;

            var list = data as IList<T> ?? data.ToList();
            if (!list.Any()) return null;
            int attempt = 0;
            var table = ToDataTable(list);
            int totalRows = table.Rows.Count;

            return table;

        }

        // ============================
        // Util: convertir lista genérica a DataTable
        // ============================
        private static DataTable ToDataTable<T>(IEnumerable<T> items)
        {
            var dt = new DataTable();
            var props = typeof(T).GetProperties();

            // Column names must match exactly the properties used in the temp table
            foreach (var p in props)
            {
                var type = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
                dt.Columns.Add(p.Name, type);
            }

            foreach (var item in items)
            {
                var values = props.Select(p => p.GetValue(item) ?? DBNull.Value).ToArray();
                dt.Rows.Add(values);
            }

            return dt;
        }



        #region ON DEMAND

        public static async Task<bool> SincronizaOnDemand(ConectDB data, OnDemandDTO dto, string sucursal, OrquestadorServidorMySQL orquestador)
        {
            using var connection = new SqlConnection(orquestador.ConnectionString);

            try
            {
                Logger.Info($"Iniciando OnDemand | Sucursal {sucursal}");

                await connection.OpenAsync();

                using var cmd = new SqlCommand("dbo.usp_OrquestadorOnDemand", connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 5000
                };

                // =========================
                // TVP: Producto
                // =========================
                if (dto.producto?.Count > 0)
                {
                    var table = await DataTableAsync(dto.producto);
                    cmd.Parameters.AddWithValue("@Producto", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@Producto"].TypeName = "dbo.TvpProducto";
                }

                // =========================
                // TVP: ProductoRecomendado
                // =========================
                if (dto.productoRecomendado?.Count > 0)
                {
                    var table = await DataTableAsync(dto.productoRecomendado);
                    cmd.Parameters.AddWithValue("@ProductoRecomendado", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@ProductoRecomendado"].TypeName = "dbo.TvpProductoRecomendado";
                }

                // =========================
                // TVP: ProductoCombo
                // =========================
                if (dto.productoCombo?.Count > 0)
                {
                    var table = await DataTableAsync(dto.productoCombo);
                    cmd.Parameters.AddWithValue("@ProductoCombo", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@ProductoCombo"].TypeName = "dbo.TvpProductoCombo";
                }

                // =========================
                // TVP: SPOSInventarios
                // =========================
                var minSqlDate = new DateTime(1753, 1, 1);

                var inv = dto.inventarios?
                    .Where(x =>
                        !string.IsNullOrWhiteSpace(x.Codigo) &&
                        x.FechaOperacion > minSqlDate
                    )
                    .ToList();

                if (inv?.Any() == true)
                {
                    inv.ForEach(x =>
                    {
                        x.IdEmpresa = orquestador.IdEmpresa;
                        x.FechaOperacion = x.FechaOperacion.Date; // opcional
                    });

                    var table = await DataTableAsync(inv);

                    cmd.Parameters.AddWithValue("@Inventarios", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@Inventarios"].TypeName = "dbo.TvpInventarios";
                }

                // =========================
                // TVP: SPOS Facturas
                // =========================
                if (dto.facturas?.Count > 0)
                {
                    dto.facturas.ForEach(x => x.IdEmpresa = orquestador.IdEmpresa);
                    var table = await DataTableAsync(dto.facturas);
                    cmd.Parameters.AddWithValue("@SPOSFacturas", table).SqlDbType = SqlDbType.Structured;
                    cmd.Parameters["@SPOSFacturas"].TypeName = "dbo.TvpSPOSFacturas";
                }

                await cmd.ExecuteNonQueryAsync();

                Logger.Info("OnDemand V2 finalizado correctamente");
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error en OnDemand V2: {ex.Message}");
                return false;
            }
        }
        #endregion



        public static async Task<bool> SincronizaHistoricosSIMIPET(ConectDB data, SalesDataDto salesDataDto, string sucursal, int id)
        {
            string connString = $"Server={data.HostName};Database={data.DatabaseName};User Id={data.UserName};Password={data.Password};TrustServerCertificate=True;Connect Timeout=60;;Max Pool Size=300;";

            using var connection = new SqlConnection(connString);
            var nombreProceso = string.Empty;

            try
            {
                Logger.Info($"Procesando la sucursal {sucursal}");
                using (var conn = new MySqlConnection(conectionSIMIPET))
                {
                    await conn.OpenAsync();
                    string query = @"UPDATE soltec2_Historicos SET Estatus = 'PROCESANDO...' WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                    using (var cmd = new MySqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
                        cmd.Parameters.AddWithValue("@IdHistorico", id);
                        await cmd.ExecuteNonQueryAsync();
                    }


                    await conn.CloseAsync();
                }

                await connection.OpenAsync();
                Logger.Info($"Iniciando SincronizaSetDeTransmisionesSQLServer - Sucursal: {sucursal}");
                int idEmpresa = 0;
                var dto = salesDataDto;
                // ============================================================
                // Preparar datos válidos y asignar IdEmpresa
                // ============================================================
                var ventasValidas = dto.Ventas?.Where(v => v.Id_Venta != null).ToList();
                ventasValidas?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasProductos = dto.VentasProductos?.Where(v => v.Id_Venta != null).ToList();
                ventasProductos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImpuestos = dto.VentasImpuestos?.Where(v => v.Id_Venta != null).ToList();
                ventasImpuestos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImpuestosDetalle = dto.VentasImpuestosDetalle?.Where(v => v.Id_Venta != null).ToList();
                ventasImpuestosDetalle?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasDesgloceTotales = dto.VentasDesgloceTotales?.Where(v => v.Id_Venta != null).ToList();
                ventasDesgloceTotales?.ForEach(x => x.IdEmpresa = idEmpresa);

                var ventasImportesProductos = dto.VentasImportesProductos?.Where(v => v.Id_Venta != null).ToList();
                ventasImportesProductos?.ForEach(x => x.IdEmpresa = idEmpresa);

                var inventarioCosto = dto.InventarioCosto?
                                                            .Where(i => !string.IsNullOrWhiteSpace(i.ClaveSimi)
                                                                     && !string.IsNullOrWhiteSpace(i.Codigo)
                                                                     && i.FechaFactura != default)
                                                            .ToList();


                var sposInventario = dto.SPOSInventario?.ToList();

                var sposFacturas = dto.SPOSFacturas?.ToList();

                var ventasVendedorCuotasConSucursal = dto.VentasVendedorCuotas
                    ?.Select(v => new VentasVendedorCuotasDto
                    {
                        ClaveSimi = data.Sucursal,
                        Fecha = v.Fecha,
                        IdVendedor = v.IdVendedor,
                        Nombre = v.Nombre,
                        ImporteVenta = v.ImporteVenta,
                        Transaccionesventa = v.Transaccionesventa,
                        PorcVenta = v.PorcVenta,
                        ImporteNaturistas = v.ImporteNaturistas,
                        PorcNaturistas = v.PorcNaturistas,
                        ImporteNocturno = v.ImporteNocturno,
                        MontoDescuento = v.MontoDescuento,
                        Menudeos = v.Menudeos,
                        MontoIva = v.MontoIva,
                        IdEmpresa = idEmpresa
                    }).ToList();

                // ============================================================
                // Ejecutar SP maestro solo si hay al menos 1 tabla con datos
                // ============================================================
                bool hayDatos = (ventasValidas?.Count > 0) ||
                                (ventasProductos?.Count > 0) ||
                                (ventasImpuestos?.Count > 0) ||
                                (ventasImpuestosDetalle?.Count > 0) ||
                                (ventasDesgloceTotales?.Count > 0) ||
                                (ventasImportesProductos?.Count > 0) ||
                                (ventasVendedorCuotasConSucursal?.Count > 0) ||
                                (inventarioCosto?.Count > 0) ||
                                (sposInventario?.Count > 0) ||
                                (sposFacturas?.Count > 0);

                if (hayDatos)
                {
                    var sw = Stopwatch.StartNew();

                    using var cmd = new SqlCommand("dbo.usp_OrquestadorVentasMaestro", connection)
                    {
                        CommandType = CommandType.StoredProcedure,
                        CommandTimeout = 1200
                    };

                    if (ventasValidas?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasValidas);
                        cmd.Parameters.AddWithValue("@Ventas", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@Ventas"].TypeName = "dbo.TvpVentas";
                    }

                    if (ventasProductos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasProductos);
                        cmd.Parameters.AddWithValue("@VentasProductos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasProductos"].TypeName = "dbo.TvpVentasProductos";
                    }

                    if (ventasImpuestos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImpuestos);
                        cmd.Parameters.AddWithValue("@VentasImpuestos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImpuestos"].TypeName = "dbo.TvpVentasImpuestos";
                    }

                    if (ventasImpuestosDetalle?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImpuestosDetalle);
                        cmd.Parameters.AddWithValue("@VentasImpuestosDetalle", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImpuestosDetalle"].TypeName = "dbo.TvpVentasImpuestosDetalle";
                    }

                    if (ventasDesgloceTotales?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasDesgloceTotales);
                        cmd.Parameters.AddWithValue("@VentasDesgloseTotales", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasDesgloseTotales"].TypeName = "dbo.TvpVentasDesgloseTotales";
                    }

                    if (ventasImportesProductos?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasImportesProductos);
                        cmd.Parameters.AddWithValue("@VentasImportesProductos", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasImportesProductos"].TypeName = "dbo.TvpVentasImportesProductos";
                    }

                    if (ventasVendedorCuotasConSucursal?.Count > 0)
                    {
                        var table = await DataTableAsync(ventasVendedorCuotasConSucursal);
                        cmd.Parameters.AddWithValue("@VentasVendedorCuotas", table).SqlDbType = SqlDbType.Structured;
                        cmd.Parameters["@VentasVendedorCuotas"].TypeName = "dbo.TvpVentasVendedorCuotas";
                    }

                    await cmd.ExecuteNonQueryAsync();

                    sw.Stop();

                    Logger.Important($"Proceso usp_OrquestadorVentasMaestro | Tiempo total: {sw.ElapsedMilliseconds} ms");
                }

                Logger.Info($"VentasVendedorCuotas procesadas.");

                connection.Dispose();


                using (var conn = new MySqlConnection(conectionSIMIPET))
                {
                    await conn.OpenAsync();
                    string query = @"UPDATE soltec2_Historicos SET Estatus = 'PROCESADO', Activo = 0, FechaProcesado=SYSDATE() WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                    using (var cmd = new MySqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
                        cmd.Parameters.AddWithValue("@IdHistorico", id);
                        await cmd.ExecuteNonQueryAsync();
                    }


                    await conn.CloseAsync();
                }

                Logger.Info($"Sincronización completada correctamente.");
                return true;
            }
            catch (Exception ex)
            {
                using (var conn = new MySqlConnection(conectionSIMIPET))
                {
                    await conn.OpenAsync();
                    string query = @"UPDATE soltec2_Historicos SET Estatus = 'RECIBIDO' WHERE ClaveSimi = @ClaveSimi AND IdHistorico=@IdHistorico";

                    using (var cmd = new MySqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@ClaveSimi", sucursal);
                        cmd.Parameters.AddWithValue("@IdHistorico", id);
                        await cmd.ExecuteNonQueryAsync();
                    }


                    await conn.CloseAsync();
                }
                Logger.Error($"Ocurrió un error: {ex.Message}");
                return false;
            }
        }
    }
}
