"""Test mocks so the exporter can be imported on a machine without HTCondor."""
import sys
import types


def _install_mock(name, attrs=None):
    mod = types.ModuleType(name)
    for k, v in (attrs or {}).items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# Minimal htcondor API surface used at import time
htcondor_mock = _install_mock("htcondor")
htcondor_mock.Collector = lambda *a, **kw: None
htcondor_mock.Schedd = lambda *a, **kw: None
htcondor_mock.AdTypes = types.SimpleNamespace(Schedd="Schedd", Startd="Startd")

# classad is also imported at module load
classad_mock = _install_mock("classad")
