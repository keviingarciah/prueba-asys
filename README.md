# Prueba Técnica: Migración de Datos de S3 a MySQL

Este proyecto implementa una solución para extraer datos de títulos de Netflix almacenados en Amazon S3, normalizarlos y cargarlos en una base de datos MySQL en la nube, para posteriormente ser consumidos por un frontend.

## Solución ETL

La solución consiste en tres componentes principales:
1. **Extracción**: Lectura de datos desde un bucket S3
2. **Transformación**: Normalización del esquema de datos
3. **Carga**: Persistencia en MySQL relacional

## Proceso ETL

### 1. Extracción de Datos

El script se conecta a Amazon S3 utilizando credenciales almacenadas en variables de entorno (.env) y extrae un archivo CSV que contiene información sobre títulos de Netflix.

```python
def read_netflix_data_from_s3():
    # Conexión a S3 y lectura del CSV
    # Retorna un DataFrame de pandas con los datos
```

### 2. Normalización de Datos

El dataset original contiene datos semi-estructurados con campos como `director`, `listed_in` y `country` que contienen valores múltiples separados por comas. Para normalizar esta estructura, se implementó el siguiente esquema relacional:

#### Modelo Entidad-Relación (ER)

La normalización sigue un modelo de entidad-relación que separa:

- **Títulos** (entidad principal)
- **Directores** (entidad secundaria)
- **Categorías** (entidad secundaria)
- **Países** (entidad secundaria)
- Relaciones muchos a muchos entre entidades

#### Tablas Creadas

- `titles`: Información principal de cada título
- `directors`: Lista normalizada de directores
- `categories`: Géneros y categorías disponibles
- `countries`: Países de producción
- `titles_directors`: Relación entre títulos y directores
- `titles_categories`: Relación entre títulos y categorías
- `titles_countries`: Relación entre títulos y países
- `netflix_titles`: Tabla pivote que mantiene los datos originales

### 3. Carga en Base de Datos

El proceso de carga incluye:

1. Creación del esquema de tablas
2. Transformación de los datos para ajustarse al esquema normalizado
3. Uso de consultas SQL avanzadas con CTEs recursivas para separar valores múltiples
4. Manejo de relaciones mediante claves foráneas

```python
def process_and_upload_data(df, engine):
    # Procesamiento y carga de datos en tablas normalizadas
```

## Aspectos Técnicos Destacados

- **Manejo de Datos Complejos**: Uso de expresiones recursivas para dividir cadenas con múltiples valores
- **Control de Transacciones**: Gestión de errores y rollback automático
- **Variables de Entorno**: Uso de `.env` para gestión segura de credenciales
- **Manipulación de Fechas**: Transformación de fechas con formato inconsistente

## Tecnologías Utilizadas

- **Python**: Lenguaje principal
- **Pandas**: Manipulación de datos
- **SQLAlchemy**: ORM para interacción con MySQL
- **Boto3**: SDK de AWS para Python
- **MySQL**: Base de datos relacional

## Conclusión

Esta solución implementa una estrategia ETL completa que convierte datos semi-estructurados en un esquema relacional optimizado, facilitando consultas complejas y mejorando el rendimiento para aplicaciones frontend que necesiten consumir esta información.

## Instrucciones de Uso

1. Configurar archivo `.env` con credenciales necesarias
2. Ejecutar el script principal
3. Verificar la carga exitosa de datos en la base MySQL
