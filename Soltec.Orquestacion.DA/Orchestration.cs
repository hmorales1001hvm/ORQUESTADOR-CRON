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
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Xml.Linq;
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

        List<OrquestadorServidorMySQL> serversDB = new List<OrquestadorServidorMySQL>();

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

                ////            stringConexion = $"Data Source={s.HostName};" +
                ////$"Initial Catalog=model;" +
                ////$"user id={s.UserName};" +
                ////$"Password={s.Password};" +
                ////$"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;";
                //using (SqlConnection connection = new SqlConnection(stringConexion))
                //{
                //	connection.Open();
                //	Logger.Info("Conexion exitosa.");
                //}



                LoadServersDB();
				foreach (var s in serversDB)
				{
					try
					{
                        stringConexion = $"Data Source=62.146.228.210;" +
                                                $"Initial Catalog=FRA_RENA;" +
                                                $"user id=orquestador;" +
                                                $"Password=S0lt3cC0nsultor3s##++;" +
                                                $"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;";

            //            stringConexion = $"Data Source={s.HostName};" +
												//$"Initial Catalog=model;" +
												//$"user id={s.UserName};" +
												//$"Password={s.Password};" +
												//$"Integrated Security=False; Persist Security Info=False;Trusted_Connection=False;TrustServerCertificate=True;";
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
					catch (Exception ex) {
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

		async void LoadServersDB()
		{
			var queryScripts = @"	SELECT B.IdEmpresa,
											HostName,
											UserName,
											Password,
											DatabaseName,
											Activo,
											Port,
											DBReference,
											UserNameReference,
											PasswordReference
									FROM soltec2_orquestador_servidormysql 					A 
									INNER JOIN soltec2_orquestador_servidormysql_detalle	B ON A.IdOrquestadorServidorMySql = B.IdOrquestadorServidorMySql WHERE B.Activo = 1;";

			try
			{
				using (var connection = new MySqlConnection(conectionString))
				{
					connection.Open();
					serversDB = connection.Query<OrquestadorServidorMySQL>(queryScripts, commandType: CommandType.Text, commandTimeout: 420).ToList();
					connection.Close();
				}

			}
			catch (Exception ex)
			{
				Logger.Error(ex.Message);
				throw;
			}
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
						parameter.Add("@idDatosTicket",t.idDatosTicket, DbType.Int32);
						parameter.Add("@sucursal",t.sucursal, DbType.String);
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
			string sql = $"	SELECT * FROM soltec2_OrquestacionClientes_Depositos WHERE Activo = 1 AND IdEmpresa IN({arrayEmpresas.Replace("[","").Replace("]", "")});";
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

			if (data.Count>0)
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

									try {
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
			} else
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
			foreach(var v in ventasDepositos)
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
            try
            {
                using var connection = new MySqlConnection(conextionStringFacturaRealOrquestador);
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



        public static async Task<bool> SincronizaHistoricos(ConectDB data, SalesDataDto salesDataDto, string sucursal, int id)
        {
            string connString = $"Server={data.HostName};Database={data.DatabaseName};User Id={data.UserName};Password={data.Password};TrustServerCertificate=True;Connect Timeout=60;;Max Pool Size=300;";

            using var connection = new SqlConnection(connString);
            var nombreProceso = string.Empty;

            try
            {
				Logger.Info($"Procesando la sucursal {sucursal}");
                using (var conn = new MySqlConnection(conextionStringFacturaRealOrquestador))
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

                // Deserializar DTO raíz
                var dto = salesDataDto; //JsonSerializer.Deserialize<SalesDataDto>(dataJSON) ?? new SalesDataDto();

                // 1) Ventas
                var ventasValidas = dto.Ventas?.Where(v => v.Id_Venta != null).ToList();
                nombreProceso = "Ventas";
                Logger.Info($"Procesando Ventas ({ventasValidas?.Count ?? 0})...");
                await BulkMergeAsync(connection, ventasValidas, @"
                CREATE TABLE #TempVentas (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    id_usuario_venta VARCHAR(50) NOT NULL,
                    Empleado VARCHAR(100) NOT NULL,
                    idRegistradora INT NOT NULL,
                    idRegistradoraVenta INT NOT NULL,
                    idRegistradoraCobro INT NOT NULL,
                    TipoOperacion INT NOT NULL,
                    Procesado SMALLINT NOT NULL,
                    FechaHoraVenta DATETIME NOT NULL,
                    TipoVenta INT NOT NULL
                );",
                    "#TempVentas",
                    @"
                MERGE INTO Ventas AS target
                USING #TempVentas AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, id_usuario_venta, Empleado,
                            idRegistradora, idRegistradoraVenta, idRegistradoraCobro, TipoOperacion,
                            Procesado, FechaHoraVenta, TipoVenta)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.id_usuario_venta, source.Empleado,
                            source.idRegistradora, source.idRegistradoraVenta, source.idRegistradoraCobro, source.TipoOperacion,
                            source.Procesado, source.FechaHoraVenta, source.TipoVenta);");

                Logger.Info($"Ventas procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 2) VentasProductos
                //var ventasProductos = dto.VentasProductos?.Where(v => v.Id_Venta != null).ToList();
                var ventasProductos = dto.VentasProductos?.Where(v => v.Id_Venta != null && !string.IsNullOrEmpty(v.Codigo)).ToList();
                Logger.Info($"Procesando VentasProductos ({ventasProductos?.Count ?? 0})...");
                nombreProceso = "VentasProductos";
                await BulkMergeAsync(connection, ventasProductos, @"
                CREATE TABLE #TempVentasProductos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Codigo CHAR(10) NOT NULL,
                    Id_ProductoSAT VARCHAR(20) NOT NULL,
                    TipoOperacion INT NOT NULL,
                    Producto VARCHAR(255) NOT NULL,
                    NoPonderado BIT NOT NULL,
                    Premio BIT NOT NULL,
                    Combo BIT NOT NULL,
                    Inventario BIT NOT NULL,
                    Cantidad DECIMAL(10,2) NOT NULL,
                    Precio DECIMAL(10,2) NOT NULL,
                    IVA DECIMAL(10,2) NOT NULL,
                    Descuento DECIMAL(10,2) NOT NULL,
                    DescuentoPorciento DECIMAL(10,2) NOT NULL,
                    IVA_Porciento DECIMAL(10,2) NOT NULL,
                    IVA_Importe DECIMAL(10,2) NOT NULL,
                    Presentacion VARCHAR(50) NULL,
                    Nivel1 VARCHAR(50) NOT NULL,
                    Nivel2 VARCHAR(50) NOT NULL,
                    Nivel3 VARCHAR(50) NOT NULL
                );",
                    "#TempVentasProductos",
                    @"
                MERGE INTO VentasProductos AS target
				USING #TempVentasProductos AS source
				ON target.FechaOperacion = source.FechaOperacion
				   AND target.ClaveSimi = source.ClaveSimi
				   AND target.Id_Venta = source.Id_Venta
				   AND target.Codigo = source.Codigo

				WHEN MATCHED THEN
					UPDATE SET
						target.Id_ProductoSAT     = source.Id_ProductoSAT,
						target.TipoOperacion      = source.TipoOperacion,
						target.Producto           = source.Producto,
						target.NoPonderado        = source.NoPonderado,
						target.Premio             = source.Premio,
						target.Combo              = source.Combo,
						target.Inventario         = source.Inventario,
						target.Cantidad           = source.Cantidad,
						target.Precio             = source.Precio,
						target.IVA                = source.IVA,
						target.Descuento          = source.Descuento,
						target.DescuentoPorciento = source.DescuentoPorciento,
						target.IVA_Porciento      = source.IVA_Porciento,
						target.IVA_Importe        = source.IVA_Importe,
						target.Presentacion       = source.Presentacion,
						target.Nivel1             = source.Nivel1,
						target.Nivel2             = source.Nivel2,
						target.Nivel3             = source.Nivel3

				WHEN NOT MATCHED THEN
					INSERT (
						FechaOperacion, ClaveSimi, Id_Venta, Codigo,
						Id_ProductoSAT, TipoOperacion, Producto,
						NoPonderado, Premio, Combo, Inventario,
						Cantidad, Precio, IVA, Descuento,
						DescuentoPorciento, IVA_Porciento, IVA_Importe,
						Presentacion, Nivel1, Nivel2, Nivel3
					)
					VALUES (
						source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Codigo,
						source.Id_ProductoSAT, source.TipoOperacion, source.Producto,
						source.NoPonderado, source.Premio, source.Combo, source.Inventario,
						source.Cantidad, source.Precio, source.IVA, source.Descuento,
						source.DescuentoPorciento, source.IVA_Porciento, source.IVA_Importe,
						source.Presentacion, source.Nivel1, source.Nivel2, source.Nivel3
					);
				");

                Logger.Info($"VentasProductos procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 3) VentasImpuestos
                var ventasImpuestos = dto.VentasImpuestos?.Where(v => v.Id_Venta != null).ToList();
                nombreProceso = "VentasImpuestos";

                Logger.Info($"Procesando VentasImpuestos ({ventasImpuestos?.Count ?? 0})...");
                await BulkMergeAsync(connection, ventasImpuestos, @"
                CREATE TABLE #TempVentasImpuestos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Impuesto VARCHAR(10) NOT NULL,
                    TipoFactor VARCHAR(10) NOT NULL,
                    TasaImpuesto NUMERIC(12,2) NOT NULL,
                    ClaveSATImpuesto VARCHAR(10) NOT NULL,
                    BaseImpuesto NUMERIC(12,2) NOT NULL,
                    ImporteImpuesto NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImpuestos",
                    @"
                MERGE INTO VentasImpuestos AS target
                USING #TempVentasImpuestos AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Impuesto = source.Impuesto
                   AND target.TipoFactor = source.TipoFactor
                   AND target.TasaImpuesto = source.TasaImpuesto
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, Impuesto, TipoFactor, TasaImpuesto, ClaveSATImpuesto, BaseImpuesto, ImporteImpuesto, TipoOperacion)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Impuesto, source.TipoFactor, source.TasaImpuesto, source.ClaveSATImpuesto, source.BaseImpuesto, source.ImporteImpuesto, source.TipoOperacion);");

                Logger.Info($"VentasImpuestos procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 4) VentasImpuestosDetalle
                var ventasImpuestosDetalle = dto.VentasImpuestosDetalle?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasImpuestosDetalle ({ventasImpuestosDetalle?.Count ?? 0})...");
                nombreProceso = "VentasImpuestosDetalle";

                await BulkMergeAsync(connection, ventasImpuestosDetalle, @"
                CREATE TABLE #TempVentasImpuestosDetalle (
                    ClaveSimi CHAR(10) NOT NULL,
                    FechaOperacion DATETIME NOT NULL,
                    Id_Venta INT NOT NULL,
                    Id_Producto VARCHAR(10) NOT NULL,
                    Impuesto VARCHAR(10) NOT NULL,
                    ClaveImpuesto VARCHAR(10) NOT NULL,
                    TasaImpuesto NUMERIC(12,2) NOT NULL,
                    TipoFactor VARCHAR(10) NOT NULL,
                    Base NUMERIC(12,2) NOT NULL,
                    ImporteIVA NUMERIC(12,2) NOT NULL,
                    ImporteVenta NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImpuestosDetalle",
                    @"
                MERGE INTO VentasImpuestosDetalle AS target
                USING #TempVentasImpuestosDetalle AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Id_Producto = source.Id_Producto
                   AND target.Impuesto = source.Impuesto
                WHEN NOT MATCHED THEN
                    INSERT (ClaveSimi, FechaOperacion, Id_Venta, Id_Producto, Impuesto, ClaveImpuesto, TasaImpuesto, TipoFactor, Base, ImporteIVA, ImporteVenta, TipoOperacion)
                    VALUES (source.ClaveSimi, source.FechaOperacion, source.Id_Venta, source.Id_Producto, source.Impuesto, source.ClaveImpuesto, source.TasaImpuesto, source.TipoFactor, source.Base, source.ImporteIVA, source.ImporteVenta, source.TipoOperacion);");

                Logger.Info($"VentasImpuestosDetalle procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 5) VentasDesgloceTotales
                var ventasDesgloceTotales = dto.VentasDesgloceTotales?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasDesgloseTotales ({ventasDesgloceTotales?.Count ?? 0})...");
                nombreProceso = "VentasDesgloseTotales";
                await BulkMergeAsync(connection, ventasDesgloceTotales, @"
                CREATE TABLE #VentasDesgloseTotales (
                    ClaveSimi CHAR(10) NOT NULL,
                    FechaOperacion DATETIME NOT NULL,
                    Id_Venta INT NOT NULL,
                    PrecioSinIVA NUMERIC(12,2) NOT NULL,
                    Importe NUMERIC(12,2) NOT NULL,
                    Descuento NUMERIC(12,2) NOT NULL,
                    Impuestos NUMERIC(12,2) NOT NULL,
                    Total NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#VentasDesgloseTotales",
                    @"
                MERGE INTO VentasDesgloseTotales AS target
                USING #VentasDesgloseTotales AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                WHEN NOT MATCHED THEN
                    INSERT (ClaveSimi, FechaOperacion, Id_Venta, PrecioSinIVA, Importe, Descuento, Impuestos, Total, TipoOperacion)
                    VALUES (source.ClaveSimi, source.FechaOperacion, source.Id_Venta, source.PrecioSinIVA, source.Importe, source.Descuento, source.Impuestos, source.Total, source.TipoOperacion);");

                Logger.Info($"VentasDesgloseTotales procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 6) VentasImportesProductos
                var ventasImportesProductos = dto.VentasImportesProductos?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasImportesProductos ({ventasImportesProductos?.Count ?? 0})...");
                nombreProceso = "VentasImportesProductos";

                await BulkMergeAsync(connection, ventasImportesProductos, @"
                CREATE TABLE #TempVentasImportesProductos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Id_Producto VARCHAR(10) NOT NULL,
                    Precio NUMERIC(12,2) NOT NULL,
                    PrecioUnitarioNeto NUMERIC(18,4) NOT NULL,
                    Cantidad INT NOT NULL,
                    SubtotalNeto NUMERIC(18,4) NOT NULL,
                    SubtotalConImpuestos NUMERIC(18,4) NOT NULL,
                    DescuentoNeto NUMERIC(18,4) NOT NULL,
                    DescuentoConImpuestos NUMERIC(18,4) NOT NULL,
                    ImporteNeto NUMERIC(12,2) NOT NULL,
                    ImporteConImpuestos NUMERIC(12,2) NOT NULL,
                    ImpuestoCalculado NUMERIC(18,4) NOT NULL,
                    Total NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImportesProductos",
                    @"
                MERGE INTO VentasImportesProductos AS target
                USING #TempVentasImportesProductos AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Id_Producto = source.Id_Producto
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, Id_Producto, Precio, PrecioUnitarioNeto, Cantidad,
                            SubtotalNeto, SubtotalConImpuestos, DescuentoNeto, DescuentoConImpuestos, ImporteNeto, ImporteConImpuestos,
                            ImpuestoCalculado, Total, TipoOperacion)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Id_Producto, source.Precio, source.PrecioUnitarioNeto, source.Cantidad,
                            source.SubtotalNeto, source.SubtotalConImpuestos, source.DescuentoNeto, source.DescuentoConImpuestos, source.ImporteNeto, source.ImporteConImpuestos,
                            source.ImpuestoCalculado, source.Total, source.TipoOperacion);");

                Logger.Info($"VentasImportesProductos procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 7) VentasVendedorCuotas
                Logger.Info($"Procesando VentasVendedorCuotas ({dto.VentasVendedorCuotas?.Count ?? 0})...");
                nombreProceso = "VentasVendedorCuotas";

                var ventasVendedorCuotasConSucursal = dto.VentasVendedorCuotas
                    .Select(v => new VentasVendedorCuotasDto
                    {
                        ClaveSimi = sucursal,
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
                        MontoIva = v.MontoIva
                    }).ToList();

                await BulkMergeAsync(connection, ventasVendedorCuotasConSucursal, @"
                                    CREATE TABLE #TempVentasVendedorCuotas (
                                        ClaveSimi VARCHAR(6) NOT NULL,
                                        Fecha DATETIME NOT NULL,
                                        IdVendedor VARCHAR(10) NOT NULL,
                                        Nombre VARCHAR(200) NOT NULL,
                                        ImporteVenta DECIMAL(12,2) NOT NULL,
                                        Transaccionesventa INT NOT NULL,
                                        PorcVenta DECIMAL(12,2) NOT NULL,
                                        ImporteNaturistas DECIMAL(12,2) NOT NULL,
                                        PorcNaturistas DECIMAL(12,2) NOT NULL,
                                        ImporteNocturno DECIMAL(12,2) NOT NULL,
                                        MontoDescuento DECIMAL(12,2) NOT NULL,
                                        Menudeos DECIMAL(12,2) NOT NULL,
                                        MontoIva DECIMAL(12,2) NOT NULL
                                    );",
                                                    "#TempVentasVendedorCuotas",
                                                    @"
                                    MERGE INTO VentasVendedorCuotas AS target
                                    USING #TempVentasVendedorCuotas AS source
                                    ON target.ClaveSimi = source.ClaveSimi
                                       AND CONVERT(VARCHAR,target.Fecha,112) = CONVERT(VARCHAR,source.Fecha,112)
                                       AND target.IdVendedor = source.IdVendedor

                                    WHEN MATCHED
                                    THEN UPDATE SET 
                                            target.Nombre = source.Nombre,
                                            target.ImporteVenta = source.ImporteVenta,
                                            target.Transaccionesventa = source.Transaccionesventa,
                                            target.PorcVenta = source.PorcVenta,
                                            target.ImporteNaturistas = source.ImporteNaturistas,
                                            target.PorcNaturistas = source.PorcNaturistas,
                                            target.ImporteNocturno = source.ImporteNocturno,
                                            target.MontoDescuento = source.MontoDescuento,
                                            target.Menudeos = source.Menudeos,
                                            target.MontoIva = source.MontoIva

                                    WHEN NOT MATCHED THEN
                                        INSERT (
                                            ClaveSimi, Fecha, IdVendedor, Nombre,
                                            ImporteVenta, Transaccionesventa, PorcVenta,
                                            ImporteNaturistas, PorcNaturistas, ImporteNocturno,
                                            MontoDescuento, Menudeos, MontoIva
                                        )
                                        VALUES (
                                            source.ClaveSimi, source.Fecha, source.IdVendedor, source.Nombre,
                                            source.ImporteVenta, source.Transaccionesventa, source.PorcVenta,
                                            source.ImporteNaturistas, source.PorcNaturistas, source.ImporteNocturno,
                                            source.MontoDescuento, source.Menudeos, source.MontoIva
                                        );
                                    ");

                Logger.Info($"VentasVendedorCuotas procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();
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


        #region ON DEMAND
        public static async Task<bool> SincronizaOnDemand(ConectDB data, OnDemandDTO dto, string sucursal)
        {
            string connString = $"Server={data.HostName};Database={data.DatabaseName};User Id={data.UserName};Password={data.Password};TrustServerCertificate=True;Connect Timeout=60;Max Pool Size=300;";
            using var connection = new SqlConnection(connString);

            try
            {
                Logger.Info($"Iniciando OnDemand | Sucursal {sucursal}");
                await connection.OpenAsync();

                await SincronizarProducto(connection, dto.producto);
                await SincronizarProductoRecomendado(connection, dto.productoRecomendado);
                await SincronizarProductoCombo(connection, dto.productoCombo);
                await SincronizarInventarios(connection, dto.inventarios, sucursal);

                Logger.Info("OnDemand finalizado correctamente");
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error en SincronizaOnDemand: {ex.Message}");
                return false;
            }
        }

        private static async Task SincronizarProducto(SqlConnection connection, List<OnDemandDTO.Producto> productos)
        {
            if (productos == null || productos.Count == 0)
                return;

            Logger.Info($"Producto ({productos.Count})");

            await BulkMergeAsync(
                connection,
                productos,
                        @"
                        CREATE TABLE #TempProducto(
                            Id_Producto VARCHAR(10),
                            Id_Nivel1 CHAR(1),
                            Id_Nivel2 CHAR(2),
                            Id_Nivel3 CHAR(4),
                            Id_Articulo INT,
                            Id_Presentacion SMALLINT,
                            Nombre VARCHAR(80),
                            MarcaEconomica VARCHAR(30),
                            PrecioCompra MONEY,
                            Precio MONEY,
                            UltimoCosto MONEY,
                            IVA SMALLMONEY,
                            Inventario BIT,
                            InventarioDiario BIT,
                            Combo BIT,
                            OTC BIT,
                            Venta BIT,
                            Servicio BIT,
                            Premio BIT,
                            EstructuraNegocio INT,
                            AplicaCaducidad BIT,
                            AplicaDescuento BIT,
                            ProductoBasico BIT,
                            AsignaPuntos SMALLINT,
                            PrecioPuntos SMALLINT,
                            ProductoGondola BIT,
                            EstatusRegistro BIT,
                            Controlado BIT,
                            Descripcion_Corta VARCHAR(20),
                            FueradeCatalogo BIT,
                            NoPonderado BIT,
                            CantidadPresentacion INT,
                            FechaInclusion DATETIME,
                            Id_ProductoSAT VARCHAR(8),
                            RequiereLote BIT,
                            IEPS SMALLMONEY
                        );",

                                "#TempProducto",

                                @"
                        MERGE Producto AS T
                        USING #TempProducto AS S
                        ON T.Id_Producto = S.Id_Producto

                        WHEN MATCHED THEN UPDATE SET
                            T.Nombre = S.Nombre,
                            T.Precio = S.Precio,
                            T.IVA = S.IVA,
                            T.Inventario = S.Inventario,
                            T.Combo = S.Combo,
                            T.OTC = S.OTC,
                            T.Premio = S.Premio,
                            T.EstatusRegistro = S.EstatusRegistro,
                            T.FueradeCatalogo = S.FueradeCatalogo,
                            T.NoPonderado = S.NoPonderado,
                            T.Id_ProductoSAT = S.Id_ProductoSAT

                        WHEN NOT MATCHED THEN
                        INSERT (
                            Id_Producto, Id_Nivel1, Id_Nivel2, Id_Nivel3,
                            Id_Articulo, Id_Presentacion, Nombre,
                            MarcaEconomica, PrecioCompra, Precio,
                            UltimoCosto, IVA, Inventario, InventarioDiario,
                            Combo, OTC, Venta, Servicio, Premio,
                            EstructuraNegocio, AplicaCaducidad, AplicaDescuento,
                            ProductoBasico, AsignaPuntos, PrecioPuntos,
                            ProductoGondola, EstatusRegistro, Controlado,
                            Descripcion_Corta, FueradeCatalogo, NoPonderado,
                            CantidadPresentacion, FechaInclusion,
                            Id_ProductoSAT, RequiereLote, IEPS
                        )
                        VALUES (
                            S.Id_Producto, S.Id_Nivel1, S.Id_Nivel2, S.Id_Nivel3,
                            S.Id_Articulo, S.Id_Presentacion, S.Nombre,
                            S.MarcaEconomica, S.PrecioCompra, S.Precio,
                            S.UltimoCosto, S.IVA, S.Inventario, S.InventarioDiario,
                            S.Combo, S.OTC, S.Venta, S.Servicio, S.Premio,
                            S.EstructuraNegocio, S.AplicaCaducidad, S.AplicaDescuento,
                            S.ProductoBasico, S.AsignaPuntos, S.PrecioPuntos,
                            S.ProductoGondola, S.EstatusRegistro, S.Controlado,
                            S.Descripcion_Corta, S.FueradeCatalogo, S.NoPonderado,
                            S.CantidadPresentacion, S.FechaInclusion,
                            S.Id_ProductoSAT, S.RequiereLote, S.IEPS);");
        }


        private static async Task SincronizarProductoRecomendado(SqlConnection connection, List<OnDemandDTO.ProductoRecomendado> lista)
        {
            if (lista == null || lista.Count == 0)
                return;

            Logger.Info($"ProductoRecomendado ({lista.Count})");

            await BulkMergeAsync(
                connection,
                lista,
                @"
                CREATE TABLE #TempProductoRecomendado(
                    Id_Producto VARCHAR(10),
                    Id_ProductoRecomendado VARCHAR(10),
                    ArgumentoPromocional VARCHAR(100),
                    Titulo VARCHAR(500),
                    Prioridad TINYINT,
                    UrlImagen VARCHAR(250),
                    EstatusRegistro BIT,
                    FechaCreacion DATETIME,
                    FechaModificacion DATETIME
                );",

                        "#TempProductoRecomendado",

                        @"
                MERGE ProductoRecomendado T
                USING #TempProductoRecomendado S
                ON T.Id_Producto = S.Id_Producto
                AND T.Id_ProductoRecomendado = S.Id_ProductoRecomendado

                WHEN MATCHED THEN UPDATE SET
                    T.ArgumentoPromocional = S.ArgumentoPromocional,
                    T.Titulo = S.Titulo,
                    T.Prioridad = S.Prioridad,
                    T.UrlImagen = S.UrlImagen,
                    T.EstatusRegistro = S.EstatusRegistro,
                    T.FechaModificacion = S.FechaModificacion

                WHEN NOT MATCHED THEN
                INSERT (
                    Id_Producto, Id_ProductoRecomendado,
                    ArgumentoPromocional, Titulo,
                    Prioridad, UrlImagen, EstatusRegistro,
                    FechaCreacion, FechaModificacion
                )
                VALUES (
                    S.Id_Producto, S.Id_ProductoRecomendado,
                    S.ArgumentoPromocional, S.Titulo,
                    S.Prioridad, S.UrlImagen, S.EstatusRegistro,
                    S.FechaCreacion, S.FechaModificacion
                );");
        }


        private static async Task SincronizarProductoCombo(SqlConnection connection, List<OnDemandDTO.Producto_Combo> combos)
        {
            if (combos == null || combos.Count == 0)
                return;

            Logger.Info($"Producto_Combo ({combos.Count})");

            await BulkMergeAsync(
                connection,
                combos,
                @"
                CREATE TABLE #TempProductoCombo(
                    Id_Producto_Combo VARCHAR(10),
                    Id_Producto VARCHAR(10),
                    Cantidad SMALLINT,
                    Descuento MONEY
                );",

                        "#TempProductoCombo",

                        @"
                MERGE Producto_Combo T
                USING #TempProductoCombo S
                ON T.Id_Producto_Combo = S.Id_Producto_Combo
                AND T.Id_Producto = S.Id_Producto

                WHEN MATCHED THEN UPDATE SET
                    T.Cantidad = S.Cantidad,
                    T.Descuento = S.Descuento

                WHEN NOT MATCHED THEN
                INSERT (
                    Id_Producto_Combo, Id_Producto,
                    Cantidad, Descuento
                )
                VALUES (
                    S.Id_Producto_Combo, S.Id_Producto,
                    S.Cantidad, S.Descuento
                );");
        }

        private static async Task SincronizarInventarios(SqlConnection connection, List<OnDemandDTO.Inventario> inventarios, string sucursal)
        {
            if (inventarios == null || inventarios.Count == 0)
            {
                Logger.Info("Inventarios: no hay registros a procesar.");
                return;
            }

            Logger.Info($"Procesando Inventarios ({inventarios.Count}) | Sucursal {sucursal}");

            // Asegurar ClaveSimi
            foreach (var inv in inventarios)
            {
                inv.ClaveSimi ??= sucursal;
            }

            await BulkMergeAsync(
                connection,
                inventarios,

                // 🔹 Tabla temporal
                @" CREATE TABLE #TempInventarios (
                ClaveSimi VARCHAR(10) NOT NULL,
                FechaOperacion DATETIME NULL,
                Id_Producto VARCHAR(10) NOT NULL,
                ExistenciaInicial INT NULL,
                Entradas INT NULL,
                Salidas INT NULL,
                ExistenciaFinal INT NULL
            );",

                    "#TempInventarios",

                    // 🔹 MERGE
                    @"
            MERGE INTO Inventarios AS target
            USING #TempInventarios AS source
            ON target.ClaveSimi = source.ClaveSimi
               AND target.Id_Producto = source.Id_Producto

            WHEN MATCHED THEN
                UPDATE SET
                    target.FechaOperacion     = source.FechaOperacion,
                    target.ExistenciaInicial  = source.ExistenciaInicial,
                    target.Entradas            = source.Entradas,
                    target.Salidas             = source.Salidas,
                    target.ExistenciaFinal     = source.ExistenciaFinal

            WHEN NOT MATCHED THEN
                INSERT (
                    ClaveSimi,
                    FechaOperacion,
                    Id_Producto,
                    ExistenciaInicial,
                    Entradas,
                    Salidas,
                    ExistenciaFinal
                )
                VALUES (
                    source.ClaveSimi,
                    source.FechaOperacion,
                    source.Id_Producto,
                    source.ExistenciaInicial,
                    source.Entradas,
                    source.Salidas,
                    source.ExistenciaFinal
                );");

            Logger.Info("Inventarios sincronizados correctamente.");
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

                // Deserializar DTO raíz
                var dto = salesDataDto; //JsonSerializer.Deserialize<SalesDataDto>(dataJSON) ?? new SalesDataDto();

                // 1) Ventas
                var ventasValidas = dto.Ventas?.Where(v => v.Id_Venta != null).ToList();
                nombreProceso = "Ventas";
                Logger.Info($"Procesando Ventas ({ventasValidas?.Count ?? 0})...");
                await BulkMergeAsync(connection, ventasValidas, @"
                CREATE TABLE #TempVentas (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    id_usuario_venta VARCHAR(50) NOT NULL,
                    Empleado VARCHAR(100) NOT NULL,
                    idRegistradora INT NOT NULL,
                    idRegistradoraVenta INT NOT NULL,
                    idRegistradoraCobro INT NOT NULL,
                    TipoOperacion INT NOT NULL,
                    Procesado SMALLINT NOT NULL,
                    FechaHoraVenta DATETIME NOT NULL,
                    TipoVenta INT NOT NULL
                );",
                    "#TempVentas",
                    @"
                MERGE INTO Ventas AS target
                USING #TempVentas AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, id_usuario_venta, Empleado,
                            idRegistradora, idRegistradoraVenta, idRegistradoraCobro, TipoOperacion,
                            Procesado, FechaHoraVenta, TipoVenta)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.id_usuario_venta, source.Empleado,
                            source.idRegistradora, source.idRegistradoraVenta, source.idRegistradoraCobro, source.TipoOperacion,
                            source.Procesado, source.FechaHoraVenta, source.TipoVenta);");

                Logger.Info($"Ventas procesadas.");

                // 2) VentasProductos
                var ventasProductos = dto.VentasProductos?.Where(v => v.Id_Venta != null && !string.IsNullOrEmpty(v.Codigo)).ToList();
                Logger.Info($"Procesando VentasProductos ({ventasProductos?.Count ?? 0})...");
                nombreProceso = "VentasProductos";
                await BulkMergeAsync(connection, ventasProductos, @"
                CREATE TABLE #TempVentasProductos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Codigo CHAR(10) NOT NULL,
                    Id_ProductoSAT VARCHAR(20) NOT NULL,
                    TipoOperacion INT NOT NULL,
                    Producto VARCHAR(255) NOT NULL,
                    NoPonderado BIT NOT NULL,
                    Premio BIT NOT NULL,
                    Combo BIT NOT NULL,
                    Inventario BIT NOT NULL,
                    Cantidad DECIMAL(10,2) NOT NULL,
                    Precio DECIMAL(10,2) NOT NULL,
                    IVA DECIMAL(10,2) NOT NULL,
                    Descuento DECIMAL(10,2) NOT NULL,
                    DescuentoPorciento DECIMAL(10,2) NOT NULL,
                    IVA_Porciento DECIMAL(10,2) NOT NULL,
                    IVA_Importe DECIMAL(10,2) NOT NULL,
                    Presentacion VARCHAR(50) NULL,
                    Nivel1 VARCHAR(50) NOT NULL,
                    Nivel2 VARCHAR(50) NOT NULL,
                    Nivel3 VARCHAR(50) NOT NULL
                );",
                    "#TempVentasProductos",
                    @"
                MERGE INTO VentasProductos AS target
				USING #TempVentasProductos AS source
				ON target.FechaOperacion = source.FechaOperacion
				   AND target.ClaveSimi = source.ClaveSimi
				   AND target.Id_Venta = source.Id_Venta
				   AND target.Codigo = source.Codigo

				WHEN MATCHED THEN
					UPDATE SET
						target.Id_ProductoSAT     = source.Id_ProductoSAT,
						target.TipoOperacion      = source.TipoOperacion,
						target.Producto           = source.Producto,
						target.NoPonderado        = source.NoPonderado,
						target.Premio             = source.Premio,
						target.Combo              = source.Combo,
						target.Inventario         = source.Inventario,
						target.Cantidad           = source.Cantidad,
						target.Precio             = source.Precio,
						target.IVA                = source.IVA,
						target.Descuento          = source.Descuento,
						target.DescuentoPorciento = source.DescuentoPorciento,
						target.IVA_Porciento      = source.IVA_Porciento,
						target.IVA_Importe        = source.IVA_Importe,
						target.Presentacion       = source.Presentacion,
						target.Nivel1             = source.Nivel1,
						target.Nivel2             = source.Nivel2,
						target.Nivel3             = source.Nivel3

				WHEN NOT MATCHED THEN
					INSERT (
						FechaOperacion, ClaveSimi, Id_Venta, Codigo,
						Id_ProductoSAT, TipoOperacion, Producto,
						NoPonderado, Premio, Combo, Inventario,
						Cantidad, Precio, IVA, Descuento,
						DescuentoPorciento, IVA_Porciento, IVA_Importe,
						Presentacion, Nivel1, Nivel2, Nivel3
					)
					VALUES (
						source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Codigo,
						source.Id_ProductoSAT, source.TipoOperacion, source.Producto,
						source.NoPonderado, source.Premio, source.Combo, source.Inventario,
						source.Cantidad, source.Precio, source.IVA, source.Descuento,
						source.DescuentoPorciento, source.IVA_Porciento, source.IVA_Importe,
						source.Presentacion, source.Nivel1, source.Nivel2, source.Nivel3
					);
				");

                Logger.Info($"VentasProductos procesadas.");

                // 3) VentasImpuestos
                var ventasImpuestos = dto.VentasImpuestos?.Where(v => v.Id_Venta != null).ToList();
                nombreProceso = "VentasImpuestos";

                Logger.Info($"Procesando VentasImpuestos ({ventasImpuestos?.Count ?? 0})...");
                await BulkMergeAsync(connection, ventasImpuestos, @"
                CREATE TABLE #TempVentasImpuestos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Impuesto VARCHAR(10) NOT NULL,
                    TipoFactor VARCHAR(10) NOT NULL,
                    TasaImpuesto NUMERIC(12,2) NOT NULL,
                    ClaveSATImpuesto VARCHAR(10) NOT NULL,
                    BaseImpuesto NUMERIC(12,2) NOT NULL,
                    ImporteImpuesto NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImpuestos",
                    @"
                MERGE INTO VentasImpuestos AS target
                USING #TempVentasImpuestos AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Impuesto = source.Impuesto
                   AND target.TipoFactor = source.TipoFactor
                   AND target.TasaImpuesto = source.TasaImpuesto
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, Impuesto, TipoFactor, TasaImpuesto, ClaveSATImpuesto, BaseImpuesto, ImporteImpuesto, TipoOperacion)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Impuesto, source.TipoFactor, source.TasaImpuesto, source.ClaveSATImpuesto, source.BaseImpuesto, source.ImporteImpuesto, source.TipoOperacion);");

                Logger.Info($"VentasImpuestos procesadas.");
                //GC.Collect();
                //GC.WaitForPendingFinalizers();

                // 4) VentasImpuestosDetalle
                var ventasImpuestosDetalle = dto.VentasImpuestosDetalle?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasImpuestosDetalle ({ventasImpuestosDetalle?.Count ?? 0})...");
                nombreProceso = "VentasImpuestosDetalle";

                await BulkMergeAsync(connection, ventasImpuestosDetalle, @"
                CREATE TABLE #TempVentasImpuestosDetalle (
                    ClaveSimi CHAR(10) NOT NULL,
                    FechaOperacion DATETIME NOT NULL,
                    Id_Venta INT NOT NULL,
                    Id_Producto VARCHAR(10) NOT NULL,
                    Impuesto VARCHAR(10) NOT NULL,
                    ClaveImpuesto VARCHAR(10) NOT NULL,
                    TasaImpuesto NUMERIC(12,2) NOT NULL,
                    TipoFactor VARCHAR(10) NOT NULL,
                    Base NUMERIC(12,2) NOT NULL,
                    ImporteIVA NUMERIC(12,2) NOT NULL,
                    ImporteVenta NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImpuestosDetalle",
                    @"
                MERGE INTO VentasImpuestosDetalle AS target
                USING #TempVentasImpuestosDetalle AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Id_Producto = source.Id_Producto
                   AND target.Impuesto = source.Impuesto
                WHEN NOT MATCHED THEN
                    INSERT (ClaveSimi, FechaOperacion, Id_Venta, Id_Producto, Impuesto, ClaveImpuesto, TasaImpuesto, TipoFactor, Base, ImporteIVA, ImporteVenta, TipoOperacion)
                    VALUES (source.ClaveSimi, source.FechaOperacion, source.Id_Venta, source.Id_Producto, source.Impuesto, source.ClaveImpuesto, source.TasaImpuesto, source.TipoFactor, source.Base, source.ImporteIVA, source.ImporteVenta, source.TipoOperacion);");

                Logger.Info($"VentasImpuestosDetalle procesadas.");

                // 5) VentasDesgloceTotales
                var ventasDesgloceTotales = dto.VentasDesgloceTotales?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasDesgloseTotales ({ventasDesgloceTotales?.Count ?? 0})...");
                nombreProceso = "VentasDesgloseTotales";
                await BulkMergeAsync(connection, ventasDesgloceTotales, @"
                CREATE TABLE #VentasDesgloseTotales (
                    ClaveSimi CHAR(10) NOT NULL,
                    FechaOperacion DATETIME NOT NULL,
                    Id_Venta INT NOT NULL,
                    PrecioSinIVA NUMERIC(12,2) NOT NULL,
                    Importe NUMERIC(12,2) NOT NULL,
                    Descuento NUMERIC(12,2) NOT NULL,
                    Impuestos NUMERIC(12,2) NOT NULL,
                    Total NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#VentasDesgloseTotales",
                    @"
                MERGE INTO VentasDesgloseTotales AS target
                USING #VentasDesgloseTotales AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                WHEN NOT MATCHED THEN
                    INSERT (ClaveSimi, FechaOperacion, Id_Venta, PrecioSinIVA, Importe, Descuento, Impuestos, Total, TipoOperacion)
                    VALUES (source.ClaveSimi, source.FechaOperacion, source.Id_Venta, source.PrecioSinIVA, source.Importe, source.Descuento, source.Impuestos, source.Total, source.TipoOperacion);");

                Logger.Info($"VentasDesgloseTotales procesadas.");

                // 6) VentasImportesProductos
                var ventasImportesProductos = dto.VentasImportesProductos?.Where(v => v.Id_Venta != null).ToList();
                Logger.Info($"Procesando VentasImportesProductos ({ventasImportesProductos?.Count ?? 0})...");
                nombreProceso = "VentasImportesProductos";

                await BulkMergeAsync(connection, ventasImportesProductos, @"
                CREATE TABLE #TempVentasImportesProductos (
                    FechaOperacion DATETIME NOT NULL,
                    ClaveSimi CHAR(10) NOT NULL,
                    Id_Venta INT NOT NULL,
                    Id_Producto VARCHAR(10) NOT NULL,
                    Precio NUMERIC(12,2) NOT NULL,
                    PrecioUnitarioNeto NUMERIC(18,4) NOT NULL,
                    Cantidad INT NOT NULL,
                    SubtotalNeto NUMERIC(18,4) NOT NULL,
                    SubtotalConImpuestos NUMERIC(18,4) NOT NULL,
                    DescuentoNeto NUMERIC(18,4) NOT NULL,
                    DescuentoConImpuestos NUMERIC(18,4) NOT NULL,
                    ImporteNeto NUMERIC(12,2) NOT NULL,
                    ImporteConImpuestos NUMERIC(12,2) NOT NULL,
                    ImpuestoCalculado NUMERIC(18,4) NOT NULL,
                    Total NUMERIC(12,2) NOT NULL,
                    TipoOperacion INT NOT NULL
                );",
                    "#TempVentasImportesProductos",
                    @"
                MERGE INTO VentasImportesProductos AS target
                USING #TempVentasImportesProductos AS source
                ON target.FechaOperacion = source.FechaOperacion
                   AND target.ClaveSimi = source.ClaveSimi
                   AND target.Id_Venta = source.Id_Venta
                   AND target.Id_Producto = source.Id_Producto
                WHEN NOT MATCHED THEN
                    INSERT (FechaOperacion, ClaveSimi, Id_Venta, Id_Producto, Precio, PrecioUnitarioNeto, Cantidad,
                            SubtotalNeto, SubtotalConImpuestos, DescuentoNeto, DescuentoConImpuestos, ImporteNeto, ImporteConImpuestos,
                            ImpuestoCalculado, Total, TipoOperacion)
                    VALUES (source.FechaOperacion, source.ClaveSimi, source.Id_Venta, source.Id_Producto, source.Precio, source.PrecioUnitarioNeto, source.Cantidad,
                            source.SubtotalNeto, source.SubtotalConImpuestos, source.DescuentoNeto, source.DescuentoConImpuestos, source.ImporteNeto, source.ImporteConImpuestos,
                            source.ImpuestoCalculado, source.Total, source.TipoOperacion);");

                Logger.Info($"VentasImportesProductos procesadas.");

                // 7) VentasVendedorCuotas
                Logger.Info($"Procesando VentasVendedorCuotas ({dto.VentasVendedorCuotas?.Count ?? 0})...");
                nombreProceso = "VentasVendedorCuotas";

                var ventasVendedorCuotasConSucursal = dto.VentasVendedorCuotas
                    .Select(v => new VentasVendedorCuotasDto
                    {
                        ClaveSimi = sucursal,
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
                        MontoIva = v.MontoIva
                    }).ToList();

                await BulkMergeAsync(connection, ventasVendedorCuotasConSucursal, @"
                                    CREATE TABLE #TempVentasVendedorCuotas (
                                        ClaveSimi VARCHAR(6) NOT NULL,
                                        Fecha DATETIME NOT NULL,
                                        IdVendedor VARCHAR(10) NOT NULL,
                                        Nombre VARCHAR(200) NOT NULL,
                                        ImporteVenta DECIMAL(12,2) NOT NULL,
                                        Transaccionesventa INT NOT NULL,
                                        PorcVenta DECIMAL(12,2) NOT NULL,
                                        ImporteNaturistas DECIMAL(12,2) NOT NULL,
                                        PorcNaturistas DECIMAL(12,2) NOT NULL,
                                        ImporteNocturno DECIMAL(12,2) NOT NULL,
                                        MontoDescuento DECIMAL(12,2) NOT NULL,
                                        Menudeos DECIMAL(12,2) NOT NULL,
                                        MontoIva DECIMAL(12,2) NOT NULL
                                    );",
                                                    "#TempVentasVendedorCuotas",
                                                    @"
                                    MERGE INTO VentasVendedorCuotas AS target
                                    USING #TempVentasVendedorCuotas AS source
                                    ON target.ClaveSimi = source.ClaveSimi
                                       AND CONVERT(VARCHAR,target.Fecha,112) = CONVERT(VARCHAR,source.Fecha,112)
                                       AND target.IdVendedor = source.IdVendedor

                                    WHEN MATCHED
                                    THEN UPDATE SET 
                                            target.Nombre = source.Nombre,
                                            target.ImporteVenta = source.ImporteVenta,
                                            target.Transaccionesventa = source.Transaccionesventa,
                                            target.PorcVenta = source.PorcVenta,
                                            target.ImporteNaturistas = source.ImporteNaturistas,
                                            target.PorcNaturistas = source.PorcNaturistas,
                                            target.ImporteNocturno = source.ImporteNocturno,
                                            target.MontoDescuento = source.MontoDescuento,
                                            target.Menudeos = source.Menudeos,
                                            target.MontoIva = source.MontoIva

                                    WHEN NOT MATCHED THEN
                                        INSERT (
                                            ClaveSimi, Fecha, IdVendedor, Nombre,
                                            ImporteVenta, Transaccionesventa, PorcVenta,
                                            ImporteNaturistas, PorcNaturistas, ImporteNocturno,
                                            MontoDescuento, Menudeos, MontoIva
                                        )
                                        VALUES (
                                            source.ClaveSimi, source.Fecha, source.IdVendedor, source.Nombre,
                                            source.ImporteVenta, source.Transaccionesventa, source.PorcVenta,
                                            source.ImporteNaturistas, source.PorcNaturistas, source.ImporteNocturno,
                                            source.MontoDescuento, source.Menudeos, source.MontoIva
                                        );
                                    ");

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


        private static async Task BulkMergeAsync<T>(
                                               SqlConnection connection,
                                               //SqlTransaction transaction,
                                               IEnumerable<T> data,
                                               string tempTableSql,
                                               string tempTableName,
                                               string mergeSql,
       int batchSizeOverride = -1)
        {
            if (data == null) return;
            var list = data as IList<T> ?? data.ToList();
            if (!list.Any()) return;

            int effectiveBatch = batchSizeOverride > 0 ? batchSizeOverride : batchSize;

            // Crear tabla temporal en la sesión (dentro de la transacción)
            using (var createCmd = new SqlCommand(tempTableSql, connection))
            {
                createCmd.CommandTimeout = 120;
                await createCmd.ExecuteNonQueryAsync();
            }

            // Convertir a DataTable (prop names => column names)
            var table = ToDataTable(list);

            // Cargar en bloques
            int totalRows = table.Rows.Count;
            int processed = 0;
            for (int i = 0; i < totalRows; i += effectiveBatch)
            {
                var rows = table.AsEnumerable().Skip(i).Take(effectiveBatch).CopyToDataTable();
                using (var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null))
                {
                    bulk.DestinationTableName = tempTableName;
                    bulk.BulkCopyTimeout = 0; // sin timeout (ajusta si lo deseas)
                    bulk.EnableStreaming = true;
                    await bulk.WriteToServerAsync(rows);
                }
                processed += rows.Rows.Count;
                //Logger.Important($"    Cargadas {processed}/{totalRows} filas en {tempTableName}");
            }

            // Ejecutar MERGE
            using (var mergeCmd = new SqlCommand(mergeSql, connection))
            {
                mergeCmd.CommandTimeout = 600;
                int affected = await mergeCmd.ExecuteNonQueryAsync();
                //Logger.Important($"    MERGE completado en {tempTableName} - filas afectadas (ExecuteNonQuery): {affected}");
            }

            // Borrar temp table (opcional, pero limpio)
            using (var dropCmd = new SqlCommand($"DROP TABLE IF EXISTS {tempTableName};", connection))
            {
                await dropCmd.ExecuteNonQueryAsync();
            }
        }


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

    }

}
