# -------------------------------------------------------------------------
# MAESTRÍA EN INTELIGENCIA ARTIFICIAL - BIG DATA
# ACTIVIDAD 8: ANÁLISIS DE VENTAS CON APACHE SPARK
# -------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc

def main():
    # 1. INICIALIZACIÓN DE LA SESIÓN DE SPARK
    # 'local[*]' indica que Spark usará todos los núcleos disponibles en tu CPU
    spark = SparkSession.builder \
        .appName("AnalisisVentasGlobales") \
        .master("local[*]") \
        .getOrCreate()

    # Ajustamos el nivel de log para no saturar la pantalla con mensajes informativos
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "="*50)
    print(" INICIANDO PROCESAMIENTO DISTRIBUIDO CON SPARK ")
    print("="*50 + "\n")

    try:
        # 2. EXTRACCIÓN (Extract)
        # Cargamos el archivo generado anteriormente
        path = "datos_ventas.csv"
        df_ventas = spark.read.csv(path, header=True, inferSchema=True)

        print(f"Dataset cargado correctamente. Total de registros: {df_ventas.count()}")
        print("Esquema de los datos:")
        df_ventas.printSchema()

        # 3. TRANSFORMACIÓN (Transform)
        # Realizamos una limpieza básica y agrupamos por país
        # Calculamos: Total de ventas, Promedio de ticket y cantidad de transacciones
        reporte_paises = df_ventas.groupBy("pais") \
            .agg(
                sum("monto").alias("Total_Ventas"),
                avg("monto").alias("Promedio_Ticket"),
                count("id_transaccion").alias("Num_Transacciones")
            ) \
            .orderBy(desc("Total_Ventas"))

        # 4. RESULTADOS Y ANÁLISIS
        print("\n--- RESULTADOS DEL ANÁLISIS POR PAÍS ---")
        reporte_paises.show()

        # Análisis adicional: Filtrar países con ventas superiores a un umbral
        # Aquí lo usaremos para demostrar el filtrado de Spark
        print("Países con mayor volumen de transacciones (> 1250):")
        reporte_paises.filter(col("Num_Transacciones") > 1250).show()

        # 5. CARGA (Load)
        # Guardamos el resultado en formato Parquet (Estándar de Big Data)
        # Esto genera una carpeta con archivos distribuidos
        reporte_paises.write.mode("overwrite").parquet("resultado_analisis.parquet")
        print("\n[OK] El reporte ha sido guardado exitosamente en formato Parquet.")

    except Exception as e:
        print(f"\n[ERROR] Ocurrió un problema durante la ejecución: {e}")

    finally:
        # Siempre cerrar la sesión de Spark al finalizar
        spark.stop()
        print("\nSesión de Spark finalizada.")

if __name__ == "__main__":
    main()