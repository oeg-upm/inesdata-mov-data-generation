# INESDATA-MOV-DATASETS

## 1. Introducción

**`inesdata_mov_datasets`** es un paquete de **Python 3.10** cuya finalidad es obtener información de fuentes de datos relacionadas con movilidad, y procesarla de tal forma que pueda ser almacenada dentro de un [espacio de datos](https://docs.internationaldataspaces.org/ids-knowledgebase/). Las fuentes de información disponibles para la extracción de datos son las siguientes:

- **EMT** ([Empresa Municipal de Transportes de Madrid](https://www.emtmadrid.es/Home)): Empresa pública que da servicio de transporte público de superficie en la ciudad de Madrid.
La EMT dispone de una [API](https://apidocs.emtmadrid.es/) desde la cual se pueden realizar peticiones para obtener información sobre las líneas de bus y sus paradas, así como los tiempos de llegada de los autobuses, entre otra información.
- **AEMET** ([Agencia Estatal de Meteorología](https://www.aemet.es/es/portada)): Agencia que ofrece información meteorológica y climatológica, predicción, avisos, observación, anuncios, atención al público, divulgación e información de la Agencia.
Al igual que la EMT, esta fuente de datos también dispone de una [API](https://opendata.aemet.es/opendata/api/) que ofrece información meteorológica dentro de la ciudad de Madrid actualizada con frecuencia horaria.
- **Informo** ([Información de Movilidad de Madrid](https://informo.madrid.es/#/realtime?panel=live)): Esta fuente de datos ofrece información sobre el tráfico de la ciudad de Madrid, llevando a cabo la medición del tráfico a través de dispositivos como las cámaras de videovigilancia instaladas en los semáforos de las calles, o a través de los lazos inductivos electromagnéticos posicionados en el pavimento de las carreteras.
Esta fuente de información también posee una [API](https://informo.madrid.es/informo/tmadrid/pm.xml) pública que ofrece información sobre el tráfico actualizada cada 5 minutos aproximadamente.

Gracias a este paquete, una vez generados los datasets dentro del espacio de datos, estos posteriormente podrían ser utilizados por ejemplo para el entrenamiento de modelos de Machine Learning.

## 2. Instalación

Clonar repositorio:

```bash
git clone https://github.com/oeg-upm/inesdata-mov-data-generation.git
```

Instalar paquete:

```bash
cd inesdata-mov-data-generation
pip install .
```

## 3. Uso

Este paquete presenta dos comandos principales: `extract` para la extracción de datos de las diferentes fuentes de información, y `create` para la creación de los distintos datasets a partir de los datos previamente extraidos.

### Comando `extract`

Comando para realizar la extracción de datos de las diferentes fuentes de información.

**Argumentos:**

- `config-path`: parámetro _obligatorio_ con la ruta al fichero de configuración YAML.
- `sources`: parámetro _opcional_ de la fuente de datos de la que se desea realizar la extracción. Los valores que puede tomar son: `emt`, `aemet`, `informo`, o `all`, que realizaría la extracción de todas las fuentes de datos disponibles. Por defecto sería `all`.

```bash
python -m inesdata_mov_datasets extract --config-path=config.yaml --sources=all
```

### Comando `create`

Comando para crear los datasets a partir de los datos previamente extraidos.

**Argumentos:**

- `config-path`: parámetro obligatorio con la ruta al fichero de configuración YAML.
- `sources`: parámetro _opcional_ de la fuente de datos de la que se desea realizar la extracción. Los valores que puede tomar son: `emt`, `aemet`, `informo`, o `all`, que realizaría la creación de los datasets de todas las fuentes disponibles. Por defecto sería `all`.
- `start-date`: parámetro _opcional_ de la fecha de inicio de la creación del dataset. Por defecto sería `datetime.today()`. El formato de dicha fecha debe ser un string con formato "YYYYMMDD".
- `end-date`: parámetro _opcional_ de la fecha de fin de la creación del dataset. Por defecto sería el día siguiente a `datetime.today()`. El formato de dicha fecha debe ser un string con formato "YYYYMMDD".

```bash
python -m inesdata_mov_datasets create --config-path=config.yaml --sources=all --start-date=20240311 --end-date=20240312
```

> ## Proyecto INESDATA
>
> [INESData](https://inesdata-project.eu/) es una Incubadora española de Espacios de Datos y Servicios de IA que utiliza infraestructuras federadas en la Nube. Se centra en simplificar la adopción de tecnología y acelerar el despliegue industrial del ecosistema nacional de espacios de datos contribuyendo con cuatro espacios de datos (idioma, movilidad, licitación pública y legal, y medios) para demostrar los beneficios de los espacios de datos y la aplicabilidad de la tecnología relacionada. Está financiado por el Ministerio de Transformación Digital de España y NextGenerationEU, en el marco del Programa UNICO I+D CLOUD - Real Decreto 959/2022.
