import pickle
from types import SimpleNamespace as NS

import pytest

from meshweaver.integration.cli import (
    build_parser, format_record, gossip_factory_for, load_callable, main,
)


def test_load_callable_finds_a_function():
    assert load_callable("math:factorial")(5) == 120


def test_load_callable_rejects_bad_specs():
    for bad in ("math", ":x", "math:", "no_such_module_xyz:f", "math:no_such_name", "math:pi"):
        with pytest.raises(ValueError):
            load_callable(bad)


def test_parser_defaults_and_subcommands():
    run = build_parser().parse_args(["run"])
    assert (run.command, run.port_base, run.ports, run.bootstrap) == ("run", 9000, 20, [])
    sub = build_parser().parse_args(["submit", "--wait", "2", "--bootstrap", "h:1"])
    assert (sub.command, sub.wait, sub.bootstrap) == ("submit", 2.0, ["h:1"])
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_format_record_for_success_and_failure():
    done = NS(status="completed", node_id="abcdef1234567890",
              result=pickle.dumps(2432902008176640000), error=None)
    assert format_record(done) == "completed on node abcdef12: 2432902008176640000"
    failed = NS(status="failed", node_id=None, result=None, error="no node available")
    assert format_record(failed) == "failed: no node available"


def test_gossip_factory_passes_the_neighbour_function_and_interval():
    class Engine:
        def __init__(self, *args, **kwargs):
            self.args, self.kwargs = args, kwargs

    neighbors = lambda: []
    engine = gossip_factory_for(Engine, 2.5)("id", "h", 1, "transport", neighbors)
    assert engine.args == ("id", "h", 1, "transport")
    assert engine.kwargs == {"get_neighbors": neighbors, "interval": 2.5}


def test_main_reports_setup_errors_without_starting_anything(monkeypatch, capsys):
    monkeypatch.setenv("MESHWEAVER_SECRET", "a-long-enough-secret")
    assert main(["run", "--gossip", "no_such_module_xyz:Engine"]) == 1
    assert "cannot load" in capsys.readouterr().err