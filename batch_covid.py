import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("COVID19_Colombia_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


df = spark.read.csv("covid_colombia.csv", header=True, inferSchema=True)
df.printSchema()
total_inicial = df.count()
print("Filas cargadas:", total_inicial)


df = df.dropDuplicates()
df = (df.withColumn("edad_anios",F.when(F.col("unidad_medida") == 1, F.col("edad").cast("int")).otherwise(0))
.withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
.withColumn("fecha_reporte", F.to_date(F.substring("fecha_reporte_web", 1, 10)))
.filter(F.col("edad_anios").between(0, 110))
.na.drop(subset=["departamento_nom", "sexo", "fecha_reporte"])
.withColumn("grupo_edad",
    F.when(F.col("edad_anios") < 18, "0-17")
        .when(F.col("edad_anios") < 40, "18-39")
        .when(F.col("edad_anios") < 60, "40-59")
        .otherwise("60+"))
.withColumn("anio_mes", F.date_format("fecha_reporte", "yyyy-MM"))
.withColumn("recuperado", F.initcap(F.trim(F.col("recuperado"))))
.withColumn("fallecido", (F.col("recuperado") == "Fallecido").cast("int")))

df.cache()
print("Filas después de limpiar:", df.count(), "de", total_inicial)

df.groupBy("recuperado").count().show()

por_depto = (df.groupBy("departamento_nom").count()
            .orderBy(F.desc("count")).limit(10))
por_sexo = df.groupBy("sexo").count()
por_mes = df.groupBy("anio_mes").count().orderBy("anio_mes")
mortalidad = (df.groupBy("grupo_edad")
            .agg(F.count("*").alias("casos"),
                    F.round(100 * F.avg("fallecido"), 2).alias("letalidad_pct"))
            .orderBy("grupo_edad"))

print("Top 10 departamentos:"); por_depto.show()
print("Casos por sexo:"); por_sexo.show()
print("Casos por mes:"); por_mes.show(50)
print("Letalidad por grupo de edad:"); mortalidad.show()
df.select("edad_anios").describe().show()

fig, ax = plt.subplots(2, 2, figsize=(14, 9))
por_depto.toPandas().plot(kind="barh", x="departamento_nom", y="count", ax=ax[0, 0], legend=False, title="Top 10 departamentos")
por_sexo.toPandas().plot(kind="bar", x="sexo", y="count", ax=ax[0, 1], legend=False, title="Casos por sexo")
por_mes.toPandas().plot(kind="bar", x="anio_mes", y="count", ax=ax[1, 0], legend=False, title="Casos por mes")
mortalidad.toPandas().plot(kind="bar", x="grupo_edad", y="letalidad_pct", ax=ax[1, 1], legend=False, title="Letalidad (%) por grupo de edad")
plt.tight_layout()
plt.savefig("resultados_batch.png", dpi=150)
print("Gráfica guardada en resultados_batch.png")
spark.stop()
