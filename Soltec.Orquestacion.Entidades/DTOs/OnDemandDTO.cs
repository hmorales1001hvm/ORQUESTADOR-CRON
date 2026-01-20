using Newtonsoft.Json;

public class OnDemandDTO
{
    [JsonProperty("ProductoRecomendado")]
    public List<ProductoRecomendado> productoRecomendado { get; set; } = new();

    [JsonProperty("Producto")]
    public List<Producto> producto { get; set; } = new();

    // 🔥 AQUÍ ESTÁ LA CLAVE
    [JsonProperty("Producto_Combo")]
    public List<Producto_Combo> productoCombo { get; set; } = new();

    public class ProductoRecomendado
    {
        public string Id_Producto { get; set; }
        public string Id_ProductoRecomendado { get; set; }
        public string ArgumentoPromocional { get; set; }
        public string Titulo { get; set; }
        public int Prioridad { get; set; }
        public string UrlImagen { get; set; }
        public bool EstatusRegistro { get; set; }
        public DateTime FechaCreacion { get; set; }
        public DateTime FechaModificacion { get; set; }
    }

    public class Producto
    {
        public string Id_Producto { get; set; }
        public string Id_Nivel1 { get; set; }
        public string Id_Nivel2 { get; set; }
        public string Id_Nivel3 { get; set; }
        public int Id_Articulo { get; set; }
        public int Id_Presentacion { get; set; }
        public string Nombre { get; set; }
        public string MarcaEconomica { get; set; }
        public decimal PrecioCompra { get; set; }
        public decimal Precio { get; set; }
        public decimal UltimoCosto { get; set; }
        public decimal IVA { get; set; }
        public bool Inventario { get; set; }
        public bool InventarioDiario { get; set; }
        public bool Combo { get; set; }
        public bool OTC { get; set; }
        public bool Venta { get; set; }
        public bool Servicio { get; set; }
        public bool Premio { get; set; }
        public int EstructuraNegocio { get; set; }
        public bool AplicaCaducidad { get; set; }
        public bool AplicaDescuento { get; set; }
        public bool ProductoBasico { get; set; }
        public int AsignaPuntos { get; set; }
        public int PrecioPuntos { get; set; }
        public bool ProductoGondola { get; set; }
        public bool EstatusRegistro { get; set; }
        public bool Controlado { get; set; }
        public string Descripcion_Corta { get; set; }
        public bool FueradeCatalogo { get; set; }
        public bool NoPonderado { get; set; }
        public int CantidadPresentacion { get; set; }
        public DateTime FechaInclusion { get; set; }
        public string Id_ProductoSAT { get; set; }
        public bool RequiereLote { get; set; }
        public decimal IEPS { get; set; }
    }

    public class Producto_Combo
    {
        public string Id_Producto_Combo { get; set; }
        public string Id_Producto { get; set; }
        public int Cantidad { get; set; }
        public decimal Descuento { get; set; }
    }
}
