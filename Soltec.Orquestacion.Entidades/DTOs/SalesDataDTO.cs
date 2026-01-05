using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Soltec.Orquestacion.Entidades.DTOs
{
    public class SalesDataDto
    {
        public List<VentasDto> Ventas { get; set; } = new();
        public List<VentasProductosDto> VentasProductos { get; set; } = new();
        public List<VentasImpuestosDto> VentasImpuestos { get; set; } = new();
        public List<VentasImpuestosDetalleDto> VentasImpuestosDetalle { get; set; } = new();
        public List<VentasDesgloceTotalesDto> VentasDesgloceTotales { get; set; } = new();
        public List<VentasImportesProductosDto> VentasImportesProductos { get; set; } = new();
        public List<VentasVendedorCuotasDto> VentasVendedorCuotas { get; set; } = new();
        public List<SPOSInventario> SPOSInventario { get; set; } = new();
        public List<InventarioCosto> InventarioCosto { get; set; } = new();
        public List<SPOSFacturas> SPOSFacturas { get; set; } = new();
    }

    public class VentasDto
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public int? Id_Venta { get; set; }
        public string id_usuario_venta { get; set; } = string.Empty;
        public string Empleado { get; set; } = string.Empty;
        public int idRegistradora { get; set; }
        public int idRegistradoraVenta { get; set; }
        public int idRegistradoraCobro { get; set; }
        public int TipoOperacion { get; set; }
        public short Procesado { get; set; } // smallint
        public DateTime FechaHoraVenta { get; set; }
        public int TipoVenta { get; set; }
    }

    public class VentasProductosDto
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public int? Id_Venta { get; set; }
        public string Codigo { get; set; } = string.Empty;
        public string Id_ProductoSAT { get; set; } = string.Empty;
        public int? TipoOperacion { get; set; }
        public string? Producto { get; set; } = string.Empty;
        [JsonConverter(typeof(FlexibleBoolConverter))]
        public bool NoPonderado { get; set; }
        [JsonConverter(typeof(FlexibleBoolConverter))]
        public bool Premio { get; set; }
        [JsonConverter(typeof(FlexibleBoolConverter))]
        public bool Combo { get; set; }
        [JsonConverter(typeof(FlexibleBoolConverter))]
        public bool Inventario { get; set; }

        // Campos que pueden venir null en JSON
        public decimal? Cantidad { get; set; }
        public decimal? Precio { get; set; }
        public decimal? IVA { get; set; }
        public decimal? Descuento { get; set; }
        public decimal? DescuentoPorciento { get; set; }
        public decimal? IVA_Porciento { get; set; }
        public decimal? IVA_Importe { get; set; }

        public string? Presentacion { get; set; } = string.Empty;
        public string Nivel1 { get; set; } = string.Empty;
        public string Nivel2 { get; set; } = string.Empty;
        public string Nivel3 { get; set; } = string.Empty;
    }

    public class VentasImpuestosDto
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public int? Id_Venta { get; set; }
        public string Impuesto { get; set; } = string.Empty;
        public string TipoFactor { get; set; } = string.Empty;
        public decimal? TasaImpuesto { get; set; }
        public string ClaveSATImpuesto { get; set; } = string.Empty;
        public decimal? BaseImpuesto { get; set; }
        public decimal? ImporteImpuesto { get; set; }
        public int? TipoOperacion { get; set; }
    }

    public class VentasImpuestosDetalleDto
    {
        public string ClaveSimi { get; set; } = string.Empty;
        public DateTime FechaOperacion { get; set; }
        public int? Id_Venta { get; set; }
        public string Id_Producto { get; set; } = string.Empty;
        public string Impuesto { get; set; } = string.Empty;
        public string ClaveImpuesto { get; set; } = string.Empty;
        public decimal? TasaImpuesto { get; set; }
        public string TipoFactor { get; set; } = string.Empty;
        public decimal? Base { get; set; }
        public decimal? ImporteIVA { get; set; }
        public decimal? ImporteVenta { get; set; }
        public int? TipoOperacion { get; set; }
    }

    public class VentasDesgloceTotalesDto
    {
        public string ClaveSimi { get; set; } = string.Empty;
        public DateTime FechaOperacion { get; set; }
        public int? Id_Venta { get; set; }
        public decimal? PrecioSinIVA { get; set; }
        public decimal? Importe { get; set; }
        public decimal? Descuento { get; set; }
        public decimal? Impuestos { get; set; }
        public decimal? Total { get; set; }
        public int? TipoOperacion { get; set; }
    }

    public class VentasImportesProductosDto
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public int? Id_Venta { get; set; }
        public string Id_Producto { get; set; } = string.Empty;
        public decimal? Precio { get; set; }
        public decimal? PrecioUnitarioNeto { get; set; }
        public decimal? Cantidad { get; set; }
        public decimal? SubtotalNeto { get; set; }
        public decimal? SubtotalConImpuestos { get; set; }
        public decimal? DescuentoNeto { get; set; }
        public decimal? DescuentoConImpuestos { get; set; }
        public decimal? ImporteNeto { get; set; }
        public decimal? ImporteConImpuestos { get; set; }
        public decimal? ImpuestoCalculado { get; set; }
        public decimal? Total { get; set; }
        public int? TipoOperacion { get; set; }
    }

    public class VentasVendedorCuotasDto
    {
        public string ClaveSimi { get; set; } = string.Empty;
        public DateTime Fecha { get; set; }
        public string? IdVendedor { get; set; } = string.Empty;
        public string? Nombre { get; set; } = string.Empty;
        public decimal? ImporteVenta { get; set; }
        public int? Transaccionesventa { get; set; }
        public decimal? PorcVenta { get; set; }
        public decimal? ImporteNaturistas { get; set; }
        public decimal? PorcNaturistas { get; set; }
        public decimal? ImporteNocturno { get; set; }
        public decimal? MontoDescuento { get; set; }
        public decimal? Menudeos { get; set; }
        public decimal? MontoIva { get; set; }
    }

    public class InventarioCosto
    {
        public string ClaveSimi { get; set; } = string.Empty;
        public string Codigo { get; set; } = string.Empty;
        public decimal? CostoUnitario { get; set; }
        public DateTime FechaFactura { get; set; }
        public DateTime? FechaSurtido { get; set; }
    }

    public class SPOSInventario
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public string Codigo { get; set; } = string.Empty;
        public string? Producto { get; set; }
        public decimal? PrecioVenta { get; set; }
        public int? ExistenciaInicial { get; set; }
        public int? ExistenciaFinal { get; set; }
        public int? Entradas { get; set; }
        public int? Salidas { get; set; }
    }

    public class SPOSFacturas
    {
        public DateTime FechaOperacion { get; set; }
        public string ClaveSimi { get; set; } = string.Empty;
        public string Serie { get; set; } = string.Empty;

        [JsonConverter(typeof(IntToStringConverter))]
        public string Folio { get; set; } = string.Empty;
        public int? Estatus { get; set; }
        public bool? Electronica { get; set; }
        public bool? NotaCredito { get; set; }
        public decimal? GranTotal { get; set; }
    }

    // Converters
    public class FlexibleBoolConverter : JsonConverter<bool>
    {
        public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var s = reader.GetString()?.ToLower();
                return s == "1" || s == "true" || s == "s" || s == "si" || s == "y" || s == "yes";
            }
            if (reader.TokenType == JsonTokenType.Number)
                return reader.GetInt32() != 0;
            if (reader.TokenType == JsonTokenType.True) return true;
            if (reader.TokenType == JsonTokenType.False) return false;

            return false;
        }

        public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
            => writer.WriteBooleanValue(value);
    }

    public class IntToStringConverter : JsonConverter<string>
    {
        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
                return reader.GetInt32().ToString();

            if (reader.TokenType == JsonTokenType.String)
                return reader.GetString();

            throw new JsonException("Tipo no válido para string");
        }

        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }
}
