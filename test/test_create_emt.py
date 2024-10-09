import pytest
import pandas as pd
import json
import os
from pathlib import Path
import tempfile
import logging
from unittest.mock import patch, mock_open, MagicMock
from pydantic import BaseModel
from inesdata_mov_datasets.sources.create.emt import generate_calendar_df_from_file, generate_calendar_day_df, create_calendar_emt, generate_line_df_from_file, generate_line_day_df, create_line_detail_emt, generate_eta_df_from_file, generate_eta_day_df, create_eta_emt, join_calendar_line_datasets, join_eta_dataset, create_emt
from inesdata_mov_datasets.settings import Settings

###################### generate_calendar_df_from_file
def test_generate_calendar_df_from_file():
    # Caso de prueba con datos válidos
    content_valid = {
        "data": [
            {"date": "2024-10-01", "dayType": "workday"},
            {"date": "2024-10-02", "dayType": "holiday"}
        ],
        "datetime": "2024-10-01 00:00:00"
    }
    
    # Llamar a la función con datos válidos
    df_valid = generate_calendar_df_from_file(content_valid)

    # Verificaciones
    assert not df_valid.empty  # El DataFrame no debe estar vacío
    assert df_valid.shape[0] == 2  # Debe haber 2 filas
    assert "datetime" in df_valid.columns  # La columna 'datetime' debe estar presente
    assert "date" in df_valid.columns  # La columna 'date' debe estar presente
    assert pd.to_datetime(df_valid["datetime"].iloc[0]) == pd.to_datetime(content_valid["datetime"])  # Verificar datetime
    assert pd.to_datetime(df_valid["date"].iloc[0]) == pd.to_datetime("2024-10-01",dayfirst=True)  # Verificar la fecha

    # Caso de prueba con datos vacíos
    content_empty = {
        "data": [],
        "datetime": "2024-10-01 00:00:00"
    }

    df_empty = generate_calendar_df_from_file(content_empty)

    # Verificaciones para datos vacíos
    assert df_empty.empty  # El DataFrame debe estar vacío

    # Caso de prueba con datos inválidos (sin la clave "data")
    content_invalid = {
        "datetime": "2024-10-01 00:00:00"
    }

    df_invalid = generate_calendar_df_from_file(content_invalid)

    # Verificaciones para datos inválidos
    assert df_invalid.empty  # El DataFrame debe estar vacío

    # Caso de prueba con datos donde ocurre una excepción
    with patch('inesdata_mov_datasets.sources.create.emt.logger') as mock_logger:  # Cambia 'inesdata_mov_datasets.sources.create.emt' por el nombre de tu módulo
        content_error = {
            "data": None,  # Esto generará un error en len()
            "datetime": "2024-10-01 00:00:00"
        }

        df_error = generate_calendar_df_from_file(content_error)

        # Verificaciones para datos que causan error
        assert df_error.empty  # El DataFrame debe estar vacío
        assert mock_logger.error.called  # Verificar que se registró un error

###################### generate_calendar_day_df
@pytest.fixture
def mock_file_content():
    # Simula el contenido de un archivo JSON
    return [{"data": [{"date": "2024-01-10", "dayType": "working"}], "datetime": "2024-01-10"}]

@pytest.fixture
def mock_storage_path(tmp_path):
    # Usa la ruta temporal proporcionada por pytest
    return str(tmp_path)

@patch("builtins.open", new_callable=mock_open)
@patch("os.listdir")
@patch("inesdata_mov_datasets.sources.create.emt.generate_calendar_df_from_file")  # Cambia 'inesdata_mov_datasets.sources.create.emt' por el nombre del módulo correcto
def test_generate_calendar_day_df(mock_generate_calendar_df_from_file, mock_listdir, mock_open, mock_storage_path, mock_file_content):
    # Configura el mock para el contenido del archivo
    mock_open.return_value.__enter__.return_value.read = lambda: json.dumps(mock_file_content)
    
    # Configura el mock para listar archivos
    mock_listdir.return_value = ["file1.json", "file2.json"]

    # Configura el mock para generate_calendar_df_from_file
    mock_generate_calendar_df_from_file.return_value = pd.DataFrame({
        "datetime": [mock_file_content[0]["datetime"]],
        "date": ["2024-01-10"],
        "dayType": ["working"]
    })

    # Llama a la función
    result_df = generate_calendar_day_df(mock_storage_path, "2024/01/10")

    # Verifica que el DataFrame generado tiene la forma correcta
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert result_df.shape[0] == 2  # Debería haber 2 filas
    assert list(result_df["dayType"]) == ["working", "working"]  # Verifica el contenido esperado

    # Verifica que las funciones de log fueron llamadas correctamente
    mock_generate_calendar_df_from_file.assert_called()
    mock_listdir.assert_called_once()

###################### create_calendar_emt
@pytest.fixture
def mock_settings_create_calendar_emt():
    """Fixture para simular la configuración de settings."""
    # Crear un mock para la clase Settings
    settings = MagicMock()
    
    # Configuración del mock
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "minio"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"

    return settings

@pytest.fixture
def mock_generate_calendar_day_df():
    with patch('inesdata_mov_datasets.sources.create.emt.generate_calendar_day_df', return_value=pd.DataFrame({"datetime": []})) as mock:
        yield mock

@pytest.fixture
def mock_async_download():
    with patch('inesdata_mov_datasets.sources.create.emt.async_download') as mock:
        yield mock

def test_create_calendar_emt(mock_settings_create_calendar_emt, mock_generate_calendar_day_df, mock_async_download):
    date = "2024/10/01"
    
    # Ejecutar la función
    df = create_calendar_emt(mock_settings_create_calendar_emt, date)
    # Verificar que async_download se llamó correctamente
    mock_async_download.assert_called_once_with(
        bucket=mock_settings_create_calendar_emt.storage.config.minio.bucket,
        prefix=f"raw/emt/{date}/calendar/",
        output_path=mock_settings_create_calendar_emt.storage.config.local.path,
        endpoint_url=mock_settings_create_calendar_emt.storage.config.minio.endpoint,
        aws_access_key_id=mock_settings_create_calendar_emt.storage.config.minio.access_key,
        aws_secret_access_key=mock_settings_create_calendar_emt.storage.config.minio.secret_key,
    )

    # Verificar que generate_calendar_day_df se llamó
    mock_generate_calendar_day_df.assert_called_once_with(storage_path="/tmp", date=date)

    # Verificar que se devolvió un DataFrame
    assert isinstance(df, pd.DataFrame)

def test_create_calendar_emt_no_download(mock_settings_create_calendar_emt, mock_generate_calendar_day_df):
    mock_settings_create_calendar_emt.storage.default = "local"  # Cambia a "local" para no descargar

    date = "2024/10/01"
    df = create_calendar_emt(mock_settings_create_calendar_emt, date)

    # Verificar que no se llamó a async_download
    with patch('inesdata_mov_datasets.sources.create.emt.async_download') as mock_async_download:
        mock_async_download.assert_not_called()
    
    # Verificar que generate_calendar_day_df se llamó
    mock_generate_calendar_day_df.assert_called_once_with(storage_path=mock_settings_create_calendar_emt.storage.config.local.path, date=date)
    assert isinstance(df, pd.DataFrame)

###################### generate_line_df_from_file
def test_generate_line_df_from_file():
    # Simular contenido de entrada
    content = {
        "data": [
            {
                "line": 10,
                "timeTable": [
                    {
                        "Direction2":
                            {"StartTime": "08:00", 
                            "StopTime": "10:00", 
                            "MinimunFrequency": 10, 
                            "MaximumFrequency": 15, 
                            "FrequencyText": "De 07:00 a 23:30 -> Cada 8 - 30min./",},
                        "Direction1":{
                                "StartTime": "08:00", 
                                "StopTime": "10:00", 
                                "MinimunFrequency": 10, 
                                "MaximumFrequency": 15,
                                "FrequencyText": "De 07:00 a 23:30 -> Cada 8 - 30min./",},  
                        "idDayType": 
                            "FESTIVO"
                    },
                ],
            }
        ],
        "datetime": "2024-10-01T00:00:00"
    }

    # Ejecutar la función
    df = generate_line_df_from_file(content)

    # Verificar que el DataFrame no está vacío
    assert not df.empty

    # Verificar las columnas del DataFrame
    expected_columns = [
        "dayType",
        "StartTime",
        "StopTime",
        "MinimunFrequency",
        "MaximumFrequency",
        "datetime",
        "date",
        "line",
    ]
    assert list(df.columns) == expected_columns
    print(df)
    # Verificar el contenido del DataFrame
    assert df["dayType"].iloc[0] == "FESTIVO"  # Correspondiente al primer elemento de Direction1
    assert df["StartTime"].iloc[0] == "08:00"
    assert df["StopTime"].iloc[0] == "10:00"
    assert df["MinimunFrequency"].iloc[0] == 10
    assert df["MaximumFrequency"].iloc[0] == 15
    assert pd.to_datetime(df["datetime"].iloc[0]) == pd.to_datetime("2024-10-01T00:00:00")
    assert pd.to_datetime(df["date"].iloc[0]).date() == pd.to_datetime("2024-10-01").date()
    assert df["line"].iloc[0] == 10  # Línea correspondiente al contenido

def test_generate_line_df_from_file_empty_data():
    # Simular contenido de entrada con datos vacíos
    content = {
        "data": [],
        "datetime": "2024-10-01T00:00:00"
    }

    # Ejecutar la función
    df = generate_line_df_from_file(content)

    # Verificar que el DataFrame está vacío
    assert df.empty

###################### generate_line_day_df
# Función simulada para generar un DataFrame a partir de contenido (mock)
def mock_generate_line_df_from_file(content):
    return pd.DataFrame({
        "dayType": [content["data"][0]["timeTable"][0]["idDayType"]],
        "StartTime": [content["data"][0]["timeTable"][0]['Direction1']["StartTime"]],
        "StopTime": [content["data"][0]["timeTable"][0]['Direction1']["StopTime"]],
        "MinimunFrequency": [content["data"][0]["timeTable"][0]['Direction1']["MinimunFrequency"]],
        "MaximumFrequency": [content["data"][0]["timeTable"][0]['Direction1']["MaximumFrequency"]],
        "datetime": [pd.to_datetime(content["datetime"])],
        "date": [pd.to_datetime(content["datetime"]).date()],
        "line": [content["data"][0]["line"]]
    })

@patch('inesdata_mov_datasets.sources.create.emt.generate_line_df_from_file', side_effect=mock_generate_line_df_from_file)
@patch('os.listdir', return_value=['file1.json', 'file2.json'])
@patch('builtins.open', new_callable=mock_open, read_data=json.dumps({
    "data": [
        {
            "line": 10,
            "timeTable": [
                {"Direction1":
                 {
                    "StartTime": "08:00",
                    "StopTime": "10:00",
                    "MinimunFrequency": 10,
                    "MaximumFrequency": 15},
                "idDayType": 1}
            ]
        }
    ],
    "datetime": "2024-10-01T00:00:00"
}))
def test_generate_line_day_df(mock_open, mock_listdir, mock_generate_line_df_from_file):
    # Establecer el path y la fecha
    storage_path = "/tmp"
    date = "2024/10/01"

    # Ejecutar la función
    df = generate_line_day_df(storage_path, date)

    # Verificar que el DataFrame no está vacío
    assert not df.empty

    # Verificar las columnas del DataFrame
    expected_columns = [
        "dayType",
        "StartTime",
        "StopTime",
        "MinimunFrequency",
        "MaximumFrequency",
        "datetime",
        "date",
        "line",
    ]
    assert list(df.columns) == expected_columns

    # Verificar el contenido del DataFrame
    assert df["dayType"].iloc[0] == 1
    assert df["StartTime"].iloc[0] == "08:00"
    assert df["StopTime"].iloc[0] == "10:00"
    assert df["MinimunFrequency"].iloc[0] == 10
    assert df["MaximumFrequency"].iloc[0] == 15
    assert pd.to_datetime(df["datetime"].iloc[0]) == pd.to_datetime("2024-10-01T00:00:00")
    assert pd.to_datetime(df["date"].iloc[0]).date() == pd.to_datetime("2024-10-01").date()
    assert df["line"].iloc[0] == 10

def test_generate_line_day_df_no_files():
    # Simular la situación donde no hay archivos en el directorio
    with patch('os.listdir', return_value=[]):
        df = generate_line_day_df("/tmp", "2024/10/01")
        assert df.empty  # El DataFrame debe estar vacío

###################### create_line_detail_emt
# Mock para el retorno de la configuración de almacenamiento
def mock_storage_settings_create_line_detail_emt():
    """Fixture para simular la configuración de settings."""
    # Crear un mock para la clase Settings
    settings = MagicMock()
    
    # Configuración del mock
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "minio"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"

    return settings

# Mock de la función de descarga
@patch('inesdata_mov_datasets.sources.create.emt.async_download')
@patch('inesdata_mov_datasets.sources.create.emt.generate_line_day_df')
def test_create_line_detail_emt(mock_generate_line_day_df, mock_async_download):
    # Preparar el mock para generate_line_day_df
    mock_generate_line_day_df.return_value = pd.DataFrame({
        "dayType": [1],
        "StartTime": ["08:00"],
        "StopTime": ["10:00"],
        "MinimunFrequency": [10],
        "MaximumFrequency": [15],
        "datetime": [pd.to_datetime("2024-10-01T00:00:00")],
        "date": [pd.to_datetime("2024-10-01").date()],
        "line": [10]
    })

    # Preparar la configuración de settings
    settings = mock_storage_settings_create_line_detail_emt()
    date = "2024/10/01"

    # Ejecutar la función
    df = create_line_detail_emt(settings, date)

    # Verificar que la función de descarga fue llamada
    mock_async_download.assert_called_once_with(
        bucket=settings.storage.config.minio.bucket,
        prefix=f"raw/emt/{date}/line_detail/",
        output_path=settings.storage.config.local.path,
        endpoint_url=settings.storage.config.minio.endpoint,
        aws_access_key_id=settings.storage.config.minio.access_key,
        aws_secret_access_key=settings.storage.config.minio.secret_key,
    )

    # Verificar que el DataFrame no está vacío
    assert not df.empty

    # Verificar las columnas del DataFrame
    expected_columns = [
        "dayType",
        "StartTime",
        "StopTime",
        "MinimunFrequency",
        "MaximumFrequency",
        "datetime",
        "date",
        "line",
    ]
    assert list(df.columns) == expected_columns

    # Verificar el contenido del DataFrame
    assert df["dayType"].iloc[0] == 1
    assert df["StartTime"].iloc[0] == "08:00"
    assert df["StopTime"].iloc[0] == "10:00"
    assert df["MinimunFrequency"].iloc[0] == 10
    assert df["MaximumFrequency"].iloc[0] == 15
    assert pd.to_datetime(df["datetime"].iloc[0]) == pd.to_datetime("2024-10-01T00:00:00")
    assert pd.to_datetime(df["date"].iloc[0]).date() == pd.to_datetime("2024-10-01").date()

###################### generate_eta_df_from_file
def test_generate_eta_df_from_file():
    # Simular contenido de entrada
    content = {
        "data": [
            {
                "Arrive": [
                    {
                        "line": 10,
                        "stop": "Main St",
                        "bus": "Bus 1",
                        "geometry": {"coordinates": [10.0, 20.0]},
                        "DistanceBus": 5.0,
                        "estimateArrive": "2024-10-01T10:00:00Z"
                    },
                    {
                        "line": 10,
                        "stop": "Second St",
                        "bus": "Bus 2",
                        "geometry": {"coordinates": [10.5, 20.5]},
                        "DistanceBus": 3.0,
                        "estimateArrive": "2024-10-01T10:15:00Z"
                    }
                ]
            }
        ],
        "datetime": "2024-10-01T09:00:00Z"
    }

    # Ejecutar la función
    df = generate_eta_df_from_file(content)

    # Verificar que el DataFrame no está vacío
    assert not df.empty

    # Verificar las columnas del DataFrame
    expected_columns = ['line', 'stop', 'bus', 'DistanceBus', 'estimateArrive', 'datetime', 'date', 'positionBusLon', 'positionBusLat']
    assert list(df.columns) == expected_columns

    # Verificar el contenido del DataFrame
    assert df["line"].iloc[0] == 10
    assert df["stop"].iloc[0] == "Main St"
    assert df["bus"].iloc[0] == "Bus 1"
    assert df["DistanceBus"].iloc[0] == 5.0
    assert pd.to_datetime(df["datetime"].iloc[0]) == pd.to_datetime("2024-10-01T09:00:00Z")
    assert df["positionBusLon"].iloc[0] == 10.0
    assert df["positionBusLat"].iloc[0] == 20.0

    # Verificar que el segundo registro tiene los valores correctos
    assert df["line"].iloc[1] == 10
    assert df["stop"].iloc[1] == "Second St"
    assert df["bus"].iloc[1] == "Bus 2"
    assert df["DistanceBus"].iloc[1] == 3.0
    assert pd.to_datetime(df["datetime"].iloc[1]) == pd.to_datetime("2024-10-01T09:00:00Z")
    assert df["positionBusLon"].iloc[1] == 10.5
    assert df["positionBusLat"].iloc[1] == 20.5


###################### generate_eta_day_df
@pytest.fixture
def mock_storage_path(tmp_path):
    """Fixture para un path de almacenamiento temporal."""
    return str(tmp_path)

@pytest.mark.asyncio
def test_generate_eta_day_df_no_files(mock_storage_path):
    """Test para verificar la generación del DataFrame de ETA cuando no hay archivos."""

    with patch('os.listdir', return_value=[]):
        result_df = generate_eta_day_df(mock_storage_path, "2024/10/08")

    # Verifica que el DataFrame resultante esté vacío
    assert result_df.empty

###################### create_eta_emt
@pytest.fixture
def settings_create_eta_emt():
    """Fixture para simular la configuración de settings."""
    # Crear un mock para la clase Settings
    settings = MagicMock()
    
    # Configuración del mock
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "minio"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"

    return settings

# Test para la función create_eta_emt
@patch('inesdata_mov_datasets.sources.create.emt.generate_eta_day_df')  
@patch('inesdata_mov_datasets.sources.create.emt.async_download')
def test_create_eta_emt(mock_async_download, mock_generate_eta_day_df, settings_create_eta_emt):
    # Configura el mock para que devuelva un DataFrame simulado
    mock_generate_eta_day_df.return_value = pd.DataFrame({
        "line": [10, 20],
        "stop": ["Main St", "Second St"],
        "bus": ["Bus 1", "Bus 2"],
        "datetime": pd.to_datetime(["2024-10-01 10:00:00", "2024-10-01 10:05:00"]),
        "date": pd.to_datetime(["2024-10-01", "2024-10-01"]),
        "DistanceBus": [5.0, 10.0],
        "positionBusLon": [10.0, 30.0],
        "positionBusLat": [20.0, 40.0]
    })

    # Llamar a la función
    result_df = create_eta_emt(settings_create_eta_emt, "2024-10-01")

    # Verifica que la función async_download fue llamada con los parámetros correctos
    mock_async_download.assert_called_once_with(
        bucket=settings_create_eta_emt.storage.config.minio.bucket,
        prefix="raw/emt/2024-10-01/eta/",
        output_path=settings_create_eta_emt.storage.config.local.path,
        endpoint_url=settings_create_eta_emt.storage.config.minio.endpoint,
        aws_access_key_id=settings_create_eta_emt.storage.config.minio.access_key,
        aws_secret_access_key=settings_create_eta_emt.storage.config.minio.secret_key,
    )

    # Verifica que el DataFrame devuelto es el esperado
    assert not result_df.empty
    assert result_df.shape == (2, 8)  # Dos filas, ocho columnas
    assert list(result_df.columns) == [
        "line", "stop", "bus", "datetime", "date", "DistanceBus", "positionBusLon", "positionBusLat"
    ]

###################### join_calendar_line_datasets
def test_join_calendar_line_datasets():
    # Crear un DataFrame de ejemplo para calendar_df
    calendar_data = {
        "date": ["2024-10-01", "2024-10-02"],
        "dayType": [1, 2],
        "datetime": ["2024-10-01 08:00:00", "2024-10-02 08:00:00"]
    }
    calendar_df = pd.DataFrame(calendar_data)
    calendar_df["date"] = pd.to_datetime(calendar_df["date"])
    calendar_df["datetime"] = pd.to_datetime(calendar_df["datetime"])

    # Crear un DataFrame de ejemplo para line_df
    line_data = {
        "date": ["2024-10-01", "2024-10-02"],
        "dayType": [1, 2],
        "datetime": ["2024-10-01 09:00:00", "2024-10-02 09:00:00"],
        "line": [101, 202],
        "StartTime": ["08:00:00", "08:30:00"],
        "StopTime": ["09:00:00", "09:30:00"]
    }
    line_df = pd.DataFrame(line_data)
    line_df["date"] = pd.to_datetime(line_df["date"])
    line_df["datetime"] = pd.to_datetime(line_df["datetime"])

    # Llamar a la función para unir los DataFrames
    result_df = join_calendar_line_datasets(calendar_df, line_df)

    # Comprobar el resultado
    expected_columns = ["date", "dayType", "datetime", "line", "StartTime", "StopTime"]
    assert list(result_df.columns) == expected_columns, "Columnas no coinciden"
    assert len(result_df) == 2, "El número de filas no es el esperado"
    assert not result_df.empty, "El DataFrame resultante está vacío"

    # Verificar que las columnas datetime se unieron correctamente
    assert all(result_df["datetime"] == line_df["datetime"]), "La columna datetime no se unió correctamente"

###################### join_eta_dataset
def test_join_eta_dataset():
    # Crear un DataFrame de ejemplo para calendar_line_df
    calendar_line_data = {
        "line": ["101", "202", "303"],
        "dayType": [1, 2, 3],
        "datetime": ["2024-10-01 08:00:00", "2024-10-02 08:30:00", "2024-10-03 09:00:00"],
        "date": ["2024-10-01", "2024-10-02", "2024-10-03"],
        "StartTime": ["08:00:00", "08:30:00", "09:00:00"],
        "StopTime": ["09:00:00", "09:30:00", "10:00:00"]
    }
    calendar_line_df = pd.DataFrame(calendar_line_data)
    calendar_line_df["datetime"] = pd.to_datetime(calendar_line_df["datetime"])
    calendar_line_df["date"] = pd.to_datetime(calendar_line_df["date"])

    # Crear un DataFrame de ejemplo para eta_df
    eta_data = {
        "line": ["101", "303", "404"],  # Línea 404 no debería hacer match
        "bus": [1, 2, 3],
        "datetime": ["2024-10-01 08:05:00", "2024-10-03 09:10:00", "2024-10-04 09:30:00"],
        "date": ["2024-10-01", "2024-10-03", "2024-10-04"],
        "estimateArrive": [300, 600, 900]
    }
    eta_df = pd.DataFrame(eta_data)
    eta_df["datetime"] = pd.to_datetime(eta_df["datetime"])
    eta_df["date"] = pd.to_datetime(eta_df["date"])

    # Llamar a la función para unir los DataFrames
    result_df = join_eta_dataset(calendar_line_df, eta_df)

    # Comprobar el resultado
    expected_columns = ["line", "bus", "datetime", "date", "estimateArrive", "dayType", "StartTime", "StopTime"]
    assert list(result_df.columns) == expected_columns, "Las columnas no coinciden con las esperadas"
    assert len(result_df) == 3, "El número de filas no es el esperado"
    
    # Verificar que la fila con la línea 404 no se haya unido (debe tener valores nulos en las columnas de calendar_line_df)
    assert pd.isna(result_df.loc[result_df["line"] == "404", "dayType"]).all(), "La fila con línea 404 no debería tener coincidencias"
    
    # Verificar que la columna `line` no tenga ceros iniciales
    assert not any(result_df["line"].str.startswith("0")), "La columna 'line' tiene valores con ceros iniciales"
    
    # Verificar que los valores datetime y date estén correctos
    assert all(result_df[result_df["line"] == "101"]["datetime"] == eta_df[eta_df["line"] == "101"]["datetime"]), "El valor de 'datetime' no coincide para la línea 101"
    assert all(result_df[result_df["line"] == "303"]["datetime"] == eta_df[eta_df["line"] == "303"]["datetime"]), "El valor de 'datetime' no coincide para la línea 303"


###################### create_emt
@pytest.fixture
def mock_settings():
    """Fixture para simular la configuración de settings."""
    # Crear un mock para la clase Settings
    settings = MagicMock()
    
    # Configuración del mock
    settings.storage = MagicMock()
    settings.storage.config = MagicMock()
    settings.storage.config.local = MagicMock()
    settings.storage.config.local.path = "/tmp"
    settings.storage.default = "local"
    settings.storage.config.minio = MagicMock()
    settings.storage.config.minio.bucket = "test-bucket"
    settings.storage.config.minio.endpoint = "http://localhost:9000"
    settings.storage.config.minio.access_key = "test-access-key"
    settings.storage.config.minio.secret_key = "test-secret-key"

    return settings

@patch('inesdata_mov_datasets.sources.create.emt.create_calendar_emt')
@patch('inesdata_mov_datasets.sources.create.emt.create_line_detail_emt')
@patch('inesdata_mov_datasets.sources.create.emt.create_eta_emt')
@patch('inesdata_mov_datasets.sources.create.emt.join_calendar_line_datasets')
@patch('inesdata_mov_datasets.sources.create.emt.join_eta_dataset')
@patch('inesdata_mov_datasets.sources.create.emt.Path.mkdir')  # Parchea la creación de directorios
@patch('inesdata_mov_datasets.sources.create.emt.pd.DataFrame.to_csv')  # Parchea la exportación a CSV
@patch('inesdata_mov_datasets.sources.create.emt.instantiate_logger')  # Parchea el logger
@pytest.mark.asyncio
def test_create_emt_success(mock_instantiate_logger, mock_to_csv, mock_mkdir, mock_join_eta, mock_join_calendar, mock_create_eta, mock_create_line_detail, mock_create_calendar, mock_settings):
    """Test para verificar la creación exitosa del dataset EMT."""

    # Simula DataFrames no vacíos
    mock_create_calendar.return_value = MagicMock(empty=False)
    mock_create_line_detail.return_value = MagicMock(empty=False)
    mock_create_eta.return_value = MagicMock(empty=False)
    
    # Simula el DataFrame final
    final_df = pd.DataFrame({
    "date": ["2024-10-08"],
    "datetime": ["2024-10-08 14:30"],
    "bus": ["Bus 1"],
    "line": ["Line A"],
    "stop": ["Stop 1"],
    "positionBusLon": [1.0],
    "positionBusLat": [1.0],
    "positionTypeBus": ["Type 1"],
    "DistanceBus": [0.5],
    "destination": ["Destination 1"],
    "deviation": [0],
    "StartTime": ["14:00"],
    "StopTime": ["15:00"],
    "MinimunFrequency": [5],
    "MaximumFrequency": [10],
    "isHead": [True],
    "dayType": ["weekday"],
    "strike": [False],
    "estimateArrive": ["14:30"],
})
    mock_join_calendar.return_value = MagicMock()
    mock_join_eta.return_value = final_df

    # Llamar a la función
    create_emt(mock_settings, "2024/10/08")
    print(mock_to_csv)
    # Verificar que se llama a to_csv para exportar el DataFrame
    mock_to_csv.assert_called_once()

    # Verificar que los DataFrames se unieron correctamente
    mock_join_calendar.assert_called_once()
    mock_join_eta.assert_called_once()

    # Verificar que se creó el directorio correspondiente
    mock_mkdir.assert_called_once()

    # Verificar que el logger fue llamado para iniciar la creación del dataset
    mock_instantiate_logger.assert_called_once()