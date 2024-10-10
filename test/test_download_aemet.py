import pytest
import asyncio
from unittest.mock import patch, AsyncMock
from inesdata_mov_datasets.sources.create.aemet import download_aemet  

###################### download_aemet
@pytest.mark.asyncio
async def test_download_aemet_success():
    """Test para verificar el comportamiento exitoso de `download_aemet`."""

    # Parámetros de prueba
    bucket = "test-bucket"
    prefix = "raw-data/"
    output_path = "/local/path/to/output"
    endpoint_url = "http://minio.example.com"
    aws_access_key_id = "test_access_key"
    aws_secret_access_key = "test_secret_key"

    with patch('inesdata_mov_datasets.sources.create.aemet.download_objs', new_callable=AsyncMock) as mock_download_objs:
        # Simula la respuesta de la función download_objs
        mock_download_objs.return_value = asyncio.Future()
        mock_download_objs.return_value.set_result(None)

        # Llamar a la función
        download_aemet(
            bucket,
            prefix,
            output_path,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key
        )
         # Verificar que download_objs fue llamado con los parámetros correctos
        mock_download_objs.assert_called_once_with(
            bucket,
            prefix,
            output_path,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key
        )

@pytest.mark.asyncio
async def test_download_aemet_exception():
    """Test para manejar excepciones en `download_aemet`."""
    
    # Parámetros de prueba
    bucket = "test-bucket"
    prefix = "raw-data/"
    output_path = "/local/path/to/output"
    endpoint_url = "http://minio.example.com"
    aws_access_key_id = "test_access_key"
    aws_secret_access_key = "test_secret_key"

    with patch('inesdata_mov_datasets.sources.create.aemet.download_objs', side_effect=Exception("Download error")) as mock_download_objs:
        # Llamar a la función y verificar que no se produzca una excepción no controlada
        try:
            download_aemet(
                bucket,
                prefix,
                output_path,
                endpoint_url,
                aws_access_key_id,
                aws_secret_access_key
            )
        except Exception:
            pytest.fail("download_aemet raised an unexpected exception")

        # Verificar que download_objs fue llamado
        mock_download_objs.assert_called_once_with(
            bucket,
            prefix,
            output_path,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key
        )