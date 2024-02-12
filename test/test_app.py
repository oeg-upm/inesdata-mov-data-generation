from typer.testing import CliRunner

from inesdata_mov_datasets.__main__ import app

runner = CliRunner()


def test_command_gather():
    result = runner.invoke(app, ["gather", "--config", "config.yaml"])
    assert result.exit_code == 0

def test_command_create():

    # if --start-date is not provided, an error (exit_code = 2) is expected.
    result = runner.invoke(app, ["create", "--config", "config.yaml"])
    assert result.exit_code == 2
    assert """Missing option '--start-date'""" in result.stdout

    # if --start-date does not match the format YYYYMMDD, an error (exit_code = 2) is expected.
    bad_date = "20221401"
    result = runner.invoke(app, ["create", "--config", "config.yaml", "--start-date", bad_date])
    assert result.exit_code == 2
    assert """Invalid value for '--start-date': '{}'""".format(bad_date) in result.stdout

    # if --config, --start-date and --end-date are provided, no error is expected.
    good_date = "20220501"
    result = runner.invoke(app, ["create", "--config", "config.yaml", "--start-date", good_date, "--end-date", good_date])
    assert result.exit_code == 0