# INESDATA-MOV-DATASETS

## 1. Introducción 📔

**`inesdata_mov_datasets`** es un paquete Python :simple-python: cuya finalidad es obtener información de fuentes de datos relacionadas con movilidad, y procesarla de tal forma que pueda ser almacenada dentro de un [espacio de datos](https://docs.internationaldataspaces.org/ids-knowledgebase/). Las fuentes de información disponibles para la extracción de datos son las siguientes:

- 🚌 **EMT** ([Empresa Municipal de Transportes de Madrid](https://www.emtmadrid.es/Home)): Empresa pública que da servicio de transporte público de superficie en la ciudad de Madrid.
La EMT dispone de una [API](https://apidocs.emtmadrid.es/) desde la cual se pueden realizar peticiones para obtener información sobre las líneas de bus y sus paradas, así como los tiempos de llegada de los autobuses, entre otra información.
- 🌥️ **AEMET** ([Agencia Estatal de Meteorología](https://www.aemet.es/es/portada)): Agencia que ofrece información meteorológica y climatológica, predicción, avisos, observación, anuncios, atención al público, divulgación e información de la Agencia.
Al igual que la EMT, esta fuente de datos también dispone de una [API](https://opendata.aemet.es/opendata/api/) que ofrece información meteorológica dentro de la ciudad de Madrid actualizada con frecuencia horaria.
- 🚦 **Informo** ([Información de Movilidad de Madrid](https://informo.madrid.es/#/realtime?panel=live)): Esta fuente de datos ofrece información sobre el tráfico de la ciudad de Madrid, llevando a cabo la medición del tráfico a través de dispositivos como las cámaras de videovigilancia instaladas en los semáforos de las calles, o a través de los lazos inductivos electromagnéticos posicionados en el pavimento de las carreteras.
Esta fuente de información también posee una [API](https://informo.madrid.es/informo/tmadrid/pm.xml) pública que ofrece información sobre el tráfico actualizada cada 5 minutos aproximadamente.

Gracias a este paquete, una vez generados los datasets dentro del espacio de datos, estos posteriormente podrían ser utilizados por ejemplo para el entrenamiento de modelos de Machine Learning.

## 2. Instalación 🛠️

Clonar repositorio:

```bash
git clone https://github.com/oeg-upm/inesdata-mov-data-generation.git
```

Instalar paquete:

```bash
cd inesdata-mov-data-generation
pip install .
```

## 3. Uso ▶️

Este paquete presenta dos comandos principales: `extract` para la extracción de datos de las diferentes fuentes de información, y `create` para la creación de los distintos datasets a partir de los datos previamente extraidos.

### Comando `extract`

Comando para realizar la extracción de datos de las diferentes fuentes de información.

**Argumentos:**

- `config-path`: parámetro _obligatorio_ con la ruta al fichero de configuración YAML.
- `sources`: parámetro _opcional_ de la fuente de datos de la que se desea realizar la extracción. Los valores que puede tomar son: `emt`, `aemet`, `informo`, o `all`, que realizaría la extracción de todas las fuentes de datos disponibles. Por defecto sería `all`.

```bash
python -m inesdata_mov_datasets extract --config-path=config.yaml --sources=all
```

??? note

    Generalmente, el uso de este comando se va a usar de forma periódica con el objetivo de crear un histórico de estas fuentes. Una sencilla forma
    de correr este comando es haciendo uso de `crontab`. El siguiente ejemplo muestra cómo se pueden generar datos cada minuto en el intervalo de 7:00 am a 21:00 pm:

    ```bash
    7-21 * * * * python -m inesdata_mov_datasets extract --config-path=config.yaml --sources=all
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

### Configuración

El fichero de configuración es donde se indica, tanto las credenciales necesarias para acceder a las fuentes, como dónde se van a guardar (1) los ficheros que se generen en el proceso. 
{ .annotate }

1.  :man_raising_hand: Actualmente se soporta tanto almacenaje remoto `s3` (minio) como en el filesystem (local)

``` yaml

sources:  # data sources credentials
  emt:  # EMT API: https://apidocs.emtmadrid.es/#api-Block_3_TRANSPORT_BUSEMTMAD
    credentials:  # EMT auth can be performed via email/password (basic) or via https://mobilitylabs.emtmadrid.es
      email: myemail  # your email for EMT basic auth
      password: mypassword  # your password for EMT basic auth
      x_client_id: my_x_client_id  # your client id for EMT mobilitylabs auth
      passkey: my_passkey  # your passkey for EMT mobilitylabs auth
    stops: [1,2]  # EMT stops ids
    lines: [1,2]  # EMT lines ids
  aemet:  # AEMET API: https://opendata.aemet.es/dist/index.html#/predicciones-especificas/Predicci%C3%B3n%20por%20municipios%20horaria.%20Tiempo%20actual.
    credentials:  # basic token auth
      api_key: my_api_key  # your api key for AEMET auth
  

storage:  # storage settings
  default: local  # default storage configuration: minio/local
  config: 
    minio:  # minio config
      access_key: my_access_key  # minio access key for auth
      secret_key: my_secret_key  # minio secret key for auth
      endpoint: minio-endpoint  # minio URL
      secure: True  # SSL
      bucket: my_bucket  # minio bucket name
    local:  # local config
      path: /path/to/save/datasets  # local storage path for resulting generated datasets
  logs:  # logging settings
    path: /path/to/save/logs  # storage path for logs
    level: LOG_LEVEL  # log level: INFO/DEBUG
```


> ## Proyecto INESDATA
>
> [INESData](https://inesdata-project.eu/) es una Incubadora española de Espacios de Datos y Servicios de IA que utiliza infraestructuras federadas en la Nube. Se centra en simplificar la adopción de tecnología y acelerar el despliegue industrial del ecosistema nacional de espacios de datos contribuyendo con cuatro espacios de datos (idioma, movilidad, licitación pública y legal, y medios) para demostrar los beneficios de los espacios de datos y la aplicabilidad de la tecnología relacionada. Está financiado por el Ministerio de Transformación Digital de España y NextGenerationEU, en el marco del Programa UNICO I+D CLOUD - Real Decreto 959/2022.
