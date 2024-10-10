from typer.testing import CliRunner

from inesdata_mov_datasets.__main__ import app

runner = CliRunner()

def test_command_create():
    # if --start-date is not provided, an error (exit_code = 2) is expected.
    result = runner.invoke(app, ["create", "--config-path", "config.yaml"])
    assert result.exit_code == 1

    # if --start-date does not match the format YYYYMMDD, an error (exit_code = 2) is expected.
    bad_start_date = "20221401"
    result = runner.invoke(app, ["create", "--config-path", "config.yaml", "--start-date", bad_start_date])
    assert result.exit_code == 2
    assert """Invalid value for '--start-date': '{}'""".format(bad_start_date) in result.stdout

    # if --end-date does not match the format YYYYMMDD, an error (exit_code = 2) is expected.
    bad_end_date = "20221401"
    result = runner.invoke(app, ["create", "--config-path", "config.yaml", "--end-date", bad_end_date])
    assert result.exit_code == 2
    assert """Invalid value for '--end-date': '{}'""".format(bad_end_date) in result.stdout

    # if --config, --start-date and --end-date are provided, no error is expected.
    good_date = "20220501"
    result = runner.invoke(app, ["create", "--config-path", "config.yaml", "--start-date", good_date, "--end-date", good_date])
    assert result.exit_code == 0

def test_command_extract():
    # if --config-path is provided, no error is expected.    
    result = runner.invoke(app, ["extract", "--config-path", "config.yaml"])
    assert result.exit_code == 0

    # if --config-path not is provided, error is expected (exit_code = 2).
    result = runner.invoke(app, ["extract"])
    assert """ Missing option '--config-path'.""" in result.stdout
    assert result.exit_code == 2

    # if --sources not in [all, emt, aemet, informo], an error (exit_code = 2) is expected.
    bad_source = "bad_source"
    result = runner.invoke(app, ["create", "--config-path", "config.yaml", "--sources", bad_source])
    assert result.exit_code == 2
    assert """Invalid value for '--sources': '{}' is not one of 'all', 'emt',""".format(bad_source) in result.stdout
