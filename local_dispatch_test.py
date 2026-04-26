import importlib.util
import json
import sys
import types
from pathlib import Path

DISPATCHER_PATH = Path(__file__).resolve().parent / "dispatcher" / "dispatcher.py"

redis_stub = types.ModuleType("redis")


class ImportRedisStub:
    def __init__(self, *args, **kwargs):
        pass


redis_stub.Redis = ImportRedisStub
sys.modules.setdefault("redis", redis_stub)

prometheus_stub = types.ModuleType("prometheus_client")


class _MetricStub:
    def labels(self, **kwargs):
        return self

    def inc(self, amount=1):
        return None

    def set(self, value):
        return None


class _RegistryStub:
    pass


prometheus_stub.CollectorRegistry = _RegistryStub
prometheus_stub.Counter = lambda *args, **kwargs: _MetricStub()
prometheus_stub.Gauge = lambda *args, **kwargs: _MetricStub()
prometheus_stub.push_to_gateway = lambda *args, **kwargs: None
sys.modules.setdefault("prometheus_client", prometheus_stub)

spec = importlib.util.spec_from_file_location("dispatcher_module", DISPATCHER_PATH)
dispatcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dispatcher)


class FakeRedis:
    def __init__(self, queue_lengths=None, busy_values=None):
        self.queue_lengths = queue_lengths or {}
        self.busy_values = busy_values or {}
        self.pushes = []

    def llen(self, queue_name):
        return self.queue_lengths.get(queue_name, 0)

    def get(self, busy_key):
        value = self.busy_values.get(busy_key)
        return None if value is None else str(value)

    def lpush(self, queue_name, payload):
        self.pushes.append((queue_name, json.loads(payload)))
        self.queue_lengths[queue_name] = self.queue_lengths.get(queue_name, 0) + 1


def reset_globals(policy, fake_redis):
    dispatcher.ROUTING_POLICY = policy
    dispatcher.r = fake_redis
    dispatcher._round_robin_index = 0
    dispatcher._tie_break_index = 0


def test_round_robin():
    fake = FakeRedis()
    reset_globals("round_robin", fake)

    jobs = [
        dispatcher.submit_job("sleep", {"duration_sec": 1}),
        dispatcher.submit_job("cpu", {"work_units": 100}),
        dispatcher.submit_job("sleep", {"duration_sec": 1}),
        dispatcher.submit_job("cpu", {"work_units": 200}),
    ]

    targets = [job["target_worker"] for job in jobs]
    assert targets == ["node2-worker", "node3-worker", "node2-worker", "node3-worker"], targets
    assert [queue for queue, _ in fake.pushes] == ["jobs-node2", "jobs-node3", "jobs-node2", "jobs-node3"]


def test_state_aware_prefers_lower_load():
    fake = FakeRedis(
        queue_lengths={"jobs-node2": 2, "jobs-node3": 0},
        busy_values={"busy-node2": 1, "busy-node3": 0},
    )
    reset_globals("state_aware", fake)

    job = dispatcher.submit_job("sleep", {"duration_sec": 1})
    assert job["target_worker"] == "node3-worker", job
    assert job["policy"] == "state_aware"
    assert job["dispatch_estimated_load_node2_worker"] == 3
    assert job["dispatch_estimated_load_node3_worker"] == 0


def test_state_aware_alternates_on_ties():
    fake = FakeRedis(
        queue_lengths={"jobs-node2": 0, "jobs-node3": 0},
        busy_values={"busy-node2": 0, "busy-node3": 0},
    )
    reset_globals("state_aware", fake)

    first = dispatcher.submit_job("sleep", {"duration_sec": 1})
    second = dispatcher.submit_job("sleep", {"duration_sec": 1})

    assert first["target_worker"] == "node2-worker", first
    assert second["target_worker"] == "node3-worker", second


if __name__ == "__main__":
    test_round_robin()
    test_state_aware_prefers_lower_load()
    test_state_aware_alternates_on_ties()
    print("LOCAL_DISPATCH_TESTS_OK")
