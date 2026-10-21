import pytest
from typer.testing import CliRunner

from velocia import __version__
from velocia.cli.app import NOT_IMPLEMENTED_EXIT, app

runner = CliRunner()

V01_COMMANDS = ["datagen", "run", "report", "doctor", "engines", "measures", "hardware"]


def test_help_lists_all_v01_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in V01_COMMANDS:
        assert command in result.output


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["datagen", "--suite", "tera", "--rows", "10"],
        ["hardware", "probe"],
    ],
)
def test_stubs_fail_loudly(args: list[str]) -> None:
    result = runner.invoke(app, args)
    assert result.exit_code == NOT_IMPLEMENTED_EXIT
    assert "Not implemented yet" in result.output
