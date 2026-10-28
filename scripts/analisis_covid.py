# Importa os para gestionar rutas, carpetas y archivos del sistema operativo.
import os

# Importa re para trabajar con expresiones regulares y normalizar textos.
import re

# Importa unicodedata para eliminar tildes y normalizar caracteres Unicode.
import unicodedata

# Importa SparkSession, que permite crear y administrar una sesión de Spark.
from pyspark.sql import SparkSession

# Importa las funciones de PySpark para transformar y analizar los datos.
from pyspark.sql import functions as F


# ==========================================================
# 1. CONFIGURACIÓN
# ==========================================================

# Define la carpeta principal donde se encuentra el proyecto.
# expanduser("~") sustituye ~ por la carpeta personal del usuario.
BASE_DIR = os.path.expanduser("~/tarea3_juangomez")

# Construye la ruta completa del archivo CSV que contiene los casos de COVID-19.
INPUT_FILE = os.path.join(
    BASE_DIR, "data", "casos_covid_colombia.csv"
)

# Define la carpeta donde se almacenarán los resultados del análisis.
OUTPUT_DIR = os.path.join(BASE_DIR, "results")


# ==========================================================
# 2. FUNCIONES PARA NORMALIZAR LOS NOMBRES DE COLUMNAS
# ==========================================================

def normalizar_nombre(nombre):
    # Descompone los caracteres Unicode para separar las letras de sus tildes.
    nombre = unicodedata.normalize("NFKD", nombre)

    # Elimina las marcas diacríticas, por ejemplo, convierte á en a.
    nombre = "".join(
        c for c in nombre
        if not unicodedata.combining(c)
    )

    # Convierte el nombre a minúsculas y elimina espacios al principio y al final.
    nombre = nombre.lower().strip()

    # Sustituye espacios, guiones y otros caracteres especiales por guiones bajos.
    # Por ejemplo: "Nombre Departamento" se convierte en "nombre_departamento".
    nombre = re.sub(r"[^a-z0-9]+", "_", nombre)

    # Elimina los guiones bajos que puedan quedar al inicio o al final.
    return nombre.strip("_")


def buscar_columna(columnas, alternativas):
    # Recorre los nombres alternativos en el orden establecido.
    for alternativa in alternativas:

        # Comprueba si el nombre alternativo existe en las columnas del DataFrame.
        if alternativa in columnas:

            # Devuelve el primer nombre de columna que encuentre.
            return alternativa

    # Si no encuentra ninguna alternativa, devuelve None.
    return None


# ==========================================================
# 3. CREAR LA SESIÓN DE SPARK
# ==========================================================

# Construye una sesión de Spark para ejecutar el procesamiento distribuido.
spark = (
    SparkSession.builder

    # Asigna un nombre identificable a la aplicación de análisis.
    .appName("AnalisisBatchCOVIDColombia")

    # Crea la sesión o reutiliza una existente si ya está disponible.
    .getOrCreate()
)

# Establece el nivel de registro en WARN para reducir los mensajes informativos.
spark.sparkContext.setLogLevel("WARN")

# try permite ejecutar el procesamiento y finally garantiza el cierre de Spark.
try:

    # Imprime un encabezado para identificar la ejecución en la terminal.
    print("\n" + "=" * 65)
    print(
        " PROCESAMIENTO BATCH - CASOS DE COVID-19 COLOMBIA - TAREA 3 - JUAN ESTEBAN GOMEZ HERNANDEZ"
    )
    print("=" * 65)

    # ======================================================
    # 4. CARGA DE DATOS
    # ======================================================

    # Comprueba que el archivo CSV exista antes de intentar leerlo.
    if not os.path.isfile(INPUT_FILE):

        # Si el archivo no existe, interrumpe el proceso con un error descriptivo.
        raise FileNotFoundError(
            f"No se encontro el archivo: {INPUT_FILE}"
        )

    print("\n[1] CARGANDO CONJUNTO DE DATOS...")

    # Lee el archivo CSV y crea un DataFrame de Spark.
    df_original = (
        spark.read

        # Indica que la primera fila del archivo contiene los encabezados.
        .option("header", True)

        # Desactiva la inferencia automática de tipos de datos.
        # Las columnas se leen inicialmente como texto.
        .option("inferSchema", False)

        # Especifica la codificación de caracteres UTF-8.
        .option("encoding", "UTF-8")

        # Permite procesar registros aunque existan problemas de formato.
        .option("mode", "PERMISSIVE")

        # Carga el archivo CSV indicado en INPUT_FILE.
        .csv(INPUT_FILE)
    )

    # Normaliza los nombres de todas las columnas para facilitar su identificación.
    nombres = [
        normalizar_nombre(c)
        for c in df_original.columns
    ]

    # Verifica que la normalización no haya generado nombres duplicados.
    # Por ejemplo, dos columnas diferentes podrían convertirse en el mismo nombre.
    if len(nombres) != len(set(nombres)):
        raise ValueError(
            "La normalizacion genero nombres de columnas duplicados."
        )

    # Asigna los nombres normalizados al DataFrame.
    df = df_original.toDF(*nombres)

    # Muestra el nombre y el tipo de dato de cada columna.
    print("\nESQUEMA DEL DATAFRAME:")
    df.printSchema()

    # Muestra todos los nombres de columnas disponibles.
    print("\nCOLUMNAS DISPONIBLES:")
    print(df.columns)

    # ======================================================
    # 5. IDENTIFICAR VARIABLES
    # ======================================================

    # Busca una columna que represente la fecha del caso.
    # Se prueban diferentes nombres porque los archivos pueden variar.
    fecha_col = buscar_columna(
        df.columns,
        [
            "fecha_notificacion",
            "fecha_de_notificacion",
            "fecha_reporte_web",
            "fecha_de_diagnostico",
            "fecha_diagnostico",
        ],
    )

    # Busca la columna que identifica el departamento de residencia o notificación.
    departamento_col = buscar_columna(
        df.columns,
        [
            "nombre_departamento",
            "departamento_nom",
            "departamento",
        ],
    )

    # Busca la columna correspondiente al municipio.
    municipio_col = buscar_columna(
        df.columns,
        [
            "nombre_municipio",
            "ciudad_municipio_nom",
            "municipio",
        ],
    )

    # Busca la columna que contiene la edad de los casos registrados.
    edad_col = buscar_columna(
        df.columns,
        ["edad"],
    )

    # Busca la columna que contiene el sexo registrado.
    sexo_col = buscar_columna(
        df.columns,
        ["sexo"],
    )

    # Busca una columna que identifique de forma única cada caso.
    id_col = buscar_columna(
        df.columns,
        ["id_de_caso", "id_caso"],
    )

    # Organiza las variables identificadas en un diccionario.
    # Cada clave representa una variable de interés y su valor es el nombre
    # real de la columna encontrada en el archivo.
    variables = {
        "fecha": fecha_col,
        "departamento": departamento_col,
        "municipio": municipio_col,
        "edad": edad_col,
        "sexo": sexo_col,
        "identificador": id_col,
    }

    # Imprime las variables identificadas para comprobar el resultado.
    print("\nVARIABLES IDENTIFICADAS:")
    for nombre, columna in variables.items():
        print(f"{nombre:18}: {columna or 'No encontrada'}")

    # La fecha es obligatoria para realizar el análisis temporal.
    if not fecha_col:
        raise ValueError(
            "No se encontro una columna de fecha compatible. "
            "Revisa los encabezados impresos anteriormente."
        )

    # El departamento es obligatorio para el análisis geográfico.
    if not departamento_col:
        raise ValueError(
            "No se encontro la columna de departamento."
        )

    # ======================================================
    # 6. ANÁLISIS EXPLORATORIO INICIAL (EDA)
    # ======================================================

    print("\n[2] EXPLORACION INICIAL")

    # Cuenta la cantidad total de registros del conjunto original.
    # count() es una acción de Spark que ejecuta el cálculo en el clúster.
    total_original = df.count()
    print(f"Registros originales: {total_original:,}")

    # Informa cuántas columnas contiene el conjunto de datos.
    print(f"Columnas: {len(df.columns)}")

    # Muestra cinco registros para observar la estructura y los valores.
    # truncate=False evita que Spark recorte el contenido de las columnas.
    print("\nMUESTRA DE REGISTROS:")
    df.show(5, truncate=False)

    # Calcula la cantidad de valores faltantes por cada columna.
    # Se considera faltante tanto un valor NULL como una cadena vacía
    # o que contenga únicamente espacios.
    print("\nVALORES FALTANTES POR COLUMNA:")

    df.select([
        F.sum(
            F.when(
                F.col(c).isNull()
                | (F.trim(F.col(c)) == ""),
                1
            ).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    # ======================================================
    # 7. LIMPIEZA DE DATOS
    # ======================================================

    print("\n[3] LIMPIEZA DE DATOS")

    # Elimina registros que sean idénticos en todas sus columnas.
    # No elimina necesariamente casos repetidos con el mismo ID si otros
    # campos de esos registros son diferentes.
    df_limpio = df.dropDuplicates()

    # Limpia las columnas de departamento, municipio y sexo cuando existan.
    for c in [departamento_col, municipio_col, sexo_col]:
        if c:

            # Convierte los valores vacíos en NULL.
            # Elimina espacios al inicio y al final y convierte el texto a mayúsculas.
            df_limpio = df_limpio.withColumn(
                c,
                F.when(
                    F.trim(F.col(c)) == "",
                    None
                ).otherwise(F.upper(F.trim(F.col(c))))
            )

    # Extrae los primeros diez caracteres de la fecha después de eliminar espacios.
    # Esto permite procesar fechas que incluyan una parte de hora después de la fecha.
    fecha_texto = F.substring(
        F.trim(F.col(fecha_col)), 1, 10
    )

    # Intenta convertir la fecha utilizando dos formatos habituales:
    # yyyy-MM-dd: año-mes-día.
    # dd/MM/yyyy: día/mes/año.
    # coalesce devuelve la primera conversión que no sea NULL.
    fecha_parseada = F.coalesce(
        F.to_date(fecha_texto, "yyyy-MM-dd"),
        F.to_date(fecha_texto, "dd/MM/yyyy"),
    )

    # Agrega una nueva columna llamada fecha_analisis con el tipo DATE.
    # Si el texto no coincide con los formatos admitidos, el resultado será NULL.
    df_limpio = df_limpio.withColumn(
        "fecha_analisis",
        fecha_parseada
    )

    # Si existe la columna de edad, intenta convertir sus valores a enteros.
    if edad_col:
        df_limpio = df_limpio.withColumn(
            "edad_numerica",

            # try_cast intenta convertir el texto a INT.
            # Si no puede realizar la conversión, devuelve NULL en vez de
            # interrumpir el procesamiento por ese valor inválido.
            F.expr(
                f"try_cast(`{edad_col}` AS INT)"
            )
        )

    # Cuenta los registros después de eliminar los duplicados exactos.
    total_limpio = df_limpio.count()

    # Informa las cantidades antes y después de la limpieza.
    print(f"Registros originales: {total_original:,}")
    print(
        f"Registros tras eliminar duplicados exactos: "
        f"{total_limpio:,}"
    )

    # Calcula la diferencia entre el conjunto original y el conjunto limpio.
    print(
        f"Duplicados exactos eliminados: "
        f"{total_original - total_limpio:,}"
    )

    # Cuenta los registros que tienen una fecha ausente o no convertible.
    print("\nREGISTROS CON FECHA INVALIDA O AUSENTE:")

    df_limpio.filter(
        F.col("fecha_analisis").isNull()
    ).count()

    # Guarda el resultado del conteo anterior para mostrarlo.
    # Esta segunda operación vuelve a ejecutar un conteo equivalente.
    invalidos = df_limpio.filter(
        F.col("fecha_analisis").isNull()
    ).count()

    print(f"Total: {invalidos:,}")

    # ======================================================
    # 8. TRANSFORMACIÓN Y ANÁLISIS TEMPORAL
    # ======================================================

    print("\n[4] CASOS POR ANO Y MES")

    # Excluye los registros sin fecha válida para el análisis temporal.
    # Después extrae el año y el mes de cada fecha.
    df_temporal = (
        df_limpio
        .filter(F.col("fecha_analisis").isNotNull())
        .withColumn("anio", F.year("fecha_analisis"))
        .withColumn("mes", F.month("fecha_analisis"))
    )

    # Agrupa los registros por año y mes.
    # Cuenta las filas de cada grupo y almacena el resultado en total_registros.
    por_periodo = (
        df_temporal
        .groupBy("anio", "mes")
        .agg(F.count("*").alias("total_registros"))

        # Ordena los resultados cronológicamente.
        .orderBy("anio", "mes")
    )

    # Muestra hasta cien combinaciones de año y mes.
    por_periodo.show(100, truncate=False)

    # ======================================================
    # 9. ANÁLISIS GEOGRÁFICO
    # ======================================================

    print("\n[5] TOP 15 DEPARTAMENTOS")

    # Excluye registros que no tienen departamento informado.
    # Agrupa por departamento y cuenta los registros de cada uno.
    por_departamento = (
        df_limpio
        .filter(F.col(departamento_col).isNotNull())
        .groupBy(departamento_col)
        .agg(F.count("*").alias("total_registros"))

        # Ordena de mayor a menor para identificar los departamentos
        # con más registros en el conjunto analizado.
        .orderBy(F.desc("total_registros"))
    )

    # Muestra los quince departamentos con más registros.
    por_departamento.show(15, truncate=False)

    # Si se encontró la columna municipio, realiza el análisis municipal.
    if municipio_col:
        print("\n[6] TOP 15 MUNICIPIOS")

        # Agrupa por departamento y municipio para distinguir municipios
        # que puedan tener nombres iguales en diferentes departamentos.
        por_municipio = (
            df_limpio
            .filter(F.col(municipio_col).isNotNull())
            .groupBy(departamento_col, municipio_col)
            .agg(F.count("*").alias("total_registros"))

            # Ordena los municipios según el número de registros.
            .orderBy(F.desc("total_registros"))
        )

        # Muestra los quince municipios con más registros.
        por_municipio.show(15, truncate=False)

    # ======================================================
    # 10. ANÁLISIS DEMOGRÁFICO
    # ======================================================

    # Si existe la variable sexo, calcula su distribución.
    if sexo_col:
        print("\n[7] DISTRIBUCION POR SEXO")

        (
            df_limpio

            # Agrupa los registros por el valor de la columna sexo.
            .groupBy(sexo_col)

            # Cuenta la cantidad de registros en cada categoría.
            .agg(F.count("*").alias("total_registros"))

            # Ordena las categorías de mayor a menor frecuencia.
            .orderBy(F.desc("total_registros"))

            # Muestra los resultados.
            .show(truncate=False)
        )

    # Si existe la variable edad, calcula la distribución por grupos etarios.
    if edad_col:
        print("\n[8] DISTRIBUCION POR GRUPOS DE EDAD")

        # Conserva las edades numéricas comprendidas entre 0 y 120 años.
        # Las edades fuera de ese intervalo y las edades nulas se excluyen.
        df_edades = (
            df_limpio
            .filter(
                F.col("edad_numerica").between(0, 120)
            )

            # Crea una nueva columna llamada grupo_edad.
            # Cada registro se clasifica según su edad.
            .withColumn(
                "grupo_edad",

                # Menores de 18 años.
                F.when(F.col("edad_numerica") < 18, "0-17")

                # De 18 a 29 años.
                .when(F.col("edad_numerica") < 30, "18-29")

                # De 30 a 44 años.
                .when(F.col("edad_numerica") < 45, "30-44")

                # De 45 a 59 años.
                .when(F.col("edad_numerica") < 60, "45-59")

                # Personas de 60 años en adelante.
                .otherwise("60+")
            )
        )

        (
            df_edades

            # Agrupa los registros por grupo de edad.
            .groupBy("grupo_edad")

            # Calcula la cantidad de registros de cada grupo.
            .agg(F.count("*").alias("total_registros"))

            # Ordena las categorías por el nombre del grupo.
            .orderBy("grupo_edad")

            # Muestra la distribución de los registros por edad.
            .show(truncate=False)
        )

    # ======================================================
    # 11. EXPORTAR LOS RESULTADOS
    # ======================================================

    print("\n[9] GUARDANDO RESULTADOS")

    # Crea la carpeta de salida si todavía no existe.
    # exist_ok=True evita un error cuando la carpeta ya está creada.
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Exporta los casos agrupados por año y mes a un archivo CSV.
    por_periodo.coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(os.path.join(OUTPUT_DIR, "casos_por_periodo"))

    # Exporta los resultados agrupados por departamento.
    por_departamento.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        os.path.join(OUTPUT_DIR, "casos_por_departamento")
    )

    # Si existe la variable municipio, exporta también su análisis.
    if municipio_col:
        por_municipio.coalesce(1).write.mode(
            "overwrite"
        ).option("header", True).csv(
            os.path.join(OUTPUT_DIR, "casos_por_municipio")
        )

    # Informa la ubicación donde se guardaron los resultados.
    print(f"Resultados guardados en: {OUTPUT_DIR}")

    # Indica que todas las etapas del procesamiento finalizaron.
    print("\nPROCESAMIENTO BATCH FINALIZADO.")

# finally se ejecuta incluso si ocurre una excepción durante el procesamiento.
finally:

    # Cierra la sesión de Spark y libera los recursos utilizados.
    spark.stop()
