# Tarea3_JuanGomez
Análisis de casos positivos de COVID-19 en Colombia mediante Apache Spark y PySpark.

# Análisis de Casos Positivos de COVID-19 en Colombia con Apache Spark

## Descripción

Este proyecto desarrolla una solución de Big Data para analizar los casos positivos de COVID-19 en Colombia mediante procesamiento por lotes (Batch Processing), utilizando Apache Spark y PySpark.

El objetivo es procesar un conjunto de datos histórico para obtener estadísticas que permitan estudiar la distribución de los casos por departamento, municipio, sexo, edad y fecha, según las operaciones implementadas en el código fuente.

## Fuente de datos

Datos Abiertos de Colombia: [Casos positivos de COVID-19 en Colombia](https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr).

Descargar el archivo CSV y ubicarlo en la ruta configurada en el script.

## Tecnologías

- Python 3
- Apache Spark 3.5.8
- PySpark
- Java 17

## Estructura del proyecto

```text
tarea3_juangomez/
├── scripts/
│   └── analisis_covid.py
├── README.md
└── .gitignore
```

## Instrucciones de ejecución

1. Instalar Java 17 y Apache Spark 3.5.8.
2. Descargar el conjunto de datos CSV y ubicarlo en la ruta esperada por el script.
3. Verificar la instalación con:

   ```bash
   java -version
   spark-submit --version
   ```

4. Desde la carpeta raíz del proyecto, ejecutar:

   ```bash
   spark-submit scripts/analisis_covid.py
   ```

5. Consultar los resultados en las rutas de salida definidas en el código.

## Procesamiento por lotes

El procesamiento batch consiste en analizar un conjunto de datos almacenado mediante una ejecución de Spark. El programa realiza las transformaciones y los cálculos estadísticos definidos en el script y genera los resultados correspondientes.

## Autor
Juan Esteban Gomez Hernandez

Curso Big Data

Grupo 122

UNAD - 2026
