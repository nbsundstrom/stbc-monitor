"""Tests for the stoomboot GPU exporter bug fixes.

These tests target three specific bugs found during investigation:
  1. stoomboot_job_memory_usage_mb was reporting `MemoryProvisioned` (the cgroup
     LIMIT, not actual usage) when schedd RSS/MemoryUsage ClassAds were missing.
  2. The Startd ClassAd fallback only queried jobs with GPUs assigned, so CPU
     jobs never got the real RSS / CPU usage from the startd.
  3. stoomboot_job_status was cleared by the personal loop but never re-emitted,
     so Prometheus reported 0 series for the detail user's CPU jobs.
"""
import os
import sys
import time
import unittest
from unittest.mock import MagicMock

# Allow `from stoomboot_gpu_exporter import ...` after conftest injects mocks
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# conftest.py injects the htcondor/classad mocks when run via pytest/unittest discover.
# When running this file directly we need to install the mocks ourselves.
if "htcondor" not in sys.modules:
    import tests.conftest  # noqa: F401  pylint: disable=import-outside-toplevel
import htcondor as _htcondor  # the mocked one from conftest


class TestActualMemoryFallback(unittest.TestCase):
    """The memory fallback chain must never return MemoryProvisioned as actual usage."""

    def test_rss_kb_takes_precedence(self):
        from stoomboot_gpu_exporter import _compute_actual_memory_mb
        job = {"ResidentSetSize": 4096, "MemoryUsage": 999,
               "MemoryProvisioned": 888, "ImageSize": 777}
        self.assertEqual(_compute_actual_memory_mb(job), 4.0)

    def test_falls_back_to_memory_usage(self):
        from stoomboot_gpu_exporter import _compute_actual_memory_mb
        job = {"MemoryUsage": 123, "MemoryProvisioned": 456, "ImageSize": 789}
        self.assertEqual(_compute_actual_memory_mb(job), 123)

    def test_does_not_use_memory_provisioned_as_actual(self):
        # The bug: schedd had no RSS/MemoryUsage/ImageSize, so the old chain
        # returned MemoryProvisioned (the cgroup limit) and the dashboard
        # showed "max all the time" (= RequestMemory).
        from stoomboot_gpu_exporter import _compute_actual_memory_mb
        job = {"MemoryProvisioned": 65536, "ImageSize": 100}
        self.assertNotEqual(_compute_actual_memory_mb(job), 65536)
        self.assertLess(_compute_actual_memory_mb(job), 1.0)

    def test_image_size_in_kb_divided_by_1024(self):
        from stoomboot_gpu_exporter import _compute_actual_memory_mb
        job = {"ImageSize": 2048}  # 2048 KB = 2 MB
        self.assertAlmostEqual(_compute_actual_memory_mb(job), 2.0)

    def test_returns_zero_when_no_data(self):
        from stoomboot_gpu_exporter import _compute_actual_memory_mb
        self.assertEqual(_compute_actual_memory_mb({}), 0)


class TestStartdMemoryFallback(unittest.TestCase):
    """The Startd fallback must populate memory_usage_mb from either
    ResidentSetSize (KB) or MemoryUsage (MB) — CPU jobs on the lot cluster
    frequently have RSS=0 in the startd ad but a real MemoryUsage value."""

    def setUp(self):
        # Invalidate the module-level cache so each test re-queries the collector
        import stoomboot_gpu_exporter as exp
        exp._htcondor_gpu_cache = {}
        exp._htcondor_gpu_cache_time = 0

    def _run(self, startd_ad):
        from stoomboot_gpu_exporter import _fetch_htcondor_gpu_metrics
        import stoomboot_gpu_exporter as exp

        coll = MagicMock()
        coll.query = MagicMock(return_value=[startd_ad])

        with unittest.mock.patch.object(exp.htcondor, "Collector", return_value=coll):
            return _fetch_htcondor_gpu_metrics(["wn-lot-007"])

    def test_uses_rss_kb_when_present(self):
        # RSS in KB is the preferred source (more accurate than MemoryUsage)
        ad = MagicMock()
        ad.get = lambda k, d=None: {
            "JobId": "100.0", "ResidentSetSize": 512 * 1024,  # 512 MB
            "MemoryUsage": 999,  # shouldn't matter — RSS wins
        }.get(k, d)
        out = self._run(ad)
        self.assertAlmostEqual(out["100.0"]["memory_usage_mb"], 512.0, places=2)

    def test_falls_back_to_memory_usage_when_rss_is_zero(self):
        # Startd's RSS is 0 for this CPU job (the bug we're fixing).
        # MemoryUsage (MB) should be used as a fallback.
        ad = MagicMock()
        ad.get = lambda k, d=None: {
            "JobId": "101.0", "ResidentSetSize": 0,
            "MemoryUsage": 256,  # MB
        }.get(k, d)
        out = self._run(ad)
        self.assertAlmostEqual(out["101.0"]["memory_usage_mb"], 256.0, places=2)

    def test_falls_back_to_memory_usage_when_rss_missing(self):
        # Startd's RSS attribute is missing entirely (some HTCondor versions
        # don't publish it for CPU-only slots).
        ad = MagicMock()
        ad.get = lambda k, d=None: {
            "JobId": "102.0", "MemoryUsage": 128,
        }.get(k, d)
        out = self._run(ad)
        self.assertAlmostEqual(out["102.0"]["memory_usage_mb"], 128.0, places=2)

    def test_no_memory_when_both_sources_missing(self):
        # Both RSS and MemoryUsage absent → no memory_usage_mb in result
        ad = MagicMock()
        ad.get = lambda k, d=None: {
            "JobId": "103.0",
        }.get(k, d)
        out = self._run(ad)
        # Caller does `htcondor_metrics.get(job_id, {})` so missing key is OK
        self.assertNotIn("memory_usage_mb", out.get("103.0", {}))


class TestStartdConstraint(unittest.TestCase):
    """The Startd fallback constraint must include CPU jobs (those without AssignedGPUs)."""

    def test_constraint_includes_cpu_jobs(self):
        from stoomboot_gpu_exporter import _build_startd_job_constraint
        c = _build_startd_job_constraint(["wn-lot-007"])
        # Must match jobs that have a JobId (which CPU jobs do, GPU jobs do)
        self.assertIn("JobId", c)
        # Must NOT filter to AssignedGPUs only — that excluded CPU jobs
        self.assertNotIn("AssignedGPUs", c)
        # Should target the requested nodes
        self.assertIn("wn-lot-007", c)

    def test_constraint_handles_multiple_nodes(self):
        from stoomboot_gpu_exporter import _build_startd_job_constraint
        c = _build_startd_job_constraint(["wn-lot-007", "wn-pijl-003"])
        self.assertIn("wn-lot-007", c)
        self.assertIn("wn-pijl-003", c)


class TestPersonalLoopRespectsStartdData(unittest.TestCase):
    """scrape_personal must not overwrite metric values that the startd fallback
    has filled in. Concretely: when the schedd's ResidentSetSize / MemoryUsage /
    TotalJobRunningCpuUsage / RemoteUserCpu / RemoteSysCpu are all zero or missing,
    the personal loop should leave the metric alone so the worker-node fallback
    (which polls every 3s) can populate it from the startd's real data.
    """

    def _run(self, running_job):
        from stoomboot_gpu_exporter import scrape_personal, job_memory_usage_mb, job_cpu_efficiency

        schedd_ad = MagicMock()
        schedd_ad.get = lambda k, d=None: {
            "Name": "schedd-fake", "MyAddress": "x", "CondorVersion": "10.0",
        }.get(k, d)

        schedd = MagicMock()
        schedd.query = MagicMock(return_value=[running_job])

        coll = MagicMock()
        coll.query = MagicMock(return_value=[schedd_ad])

        # Pre-populate the metric as if the startd fallback already filled it in
        job_memory_usage_mb.labels(
            cluster="cpu", user="testuser", job_id="100.0",
            resource_type="CPU", node="wn-lot-007",
        ).set(238.16)
        job_cpu_efficiency.labels(
            cluster="cpu", user="testuser", job_id="100.0",
            resource_type="CPU", node="wn-lot-007",
        ).set(0.518)

        with unittest.mock.patch("stoomboot_gpu_exporter.htcondor") as ht_mock:
            ht_mock.Collector = MagicMock(return_value=coll)
            ht_mock.Schedd = MagicMock(return_value=schedd)
            ht_mock.AdTypes = _htcondor.AdTypes
            scrape_personal("fake-collector", "testuser")

        mem = job_memory_usage_mb.labels(
            cluster="cpu", user="testuser", job_id="100.0",
            resource_type="CPU", node="wn-lot-007",
        )._value.get()
        cpu = job_cpu_efficiency.labels(
            cluster="cpu", user="testuser", job_id="100.0",
            resource_type="CPU", node="wn-lot-007",
        )._value.get()
        return mem, cpu

    def test_does_not_overwrite_memory_when_schedd_has_no_rss(self):
        # Schedd has no RSS/MemoryUsage (the user's CPU job 4861263.0 case).
        # The startd fallback has set memory to 238 MB — keep that.
        job = MagicMock()
        job.get = lambda k, d=None: {
            "ClusterId": 100, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 16, "RequestMemory": 65536,
            "MemoryUsage": 0, "ImageSize": 1, "ResidentSetSize": 0,
            "MemoryProvisioned": 65536, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_5@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        job.eval = lambda k: job.get(k)

        mem, _cpu = self._run(job)
        self.assertAlmostEqual(mem, 238.16, places=2)

    def test_does_not_overwrite_cpu_efficiency_when_schedd_has_no_cpu(self):
        # Schedd has no RemoteUserCpu/RemoteSysCpu (the user's CPU job case).
        # The startd fallback has set efficiency to 0.518 — keep that.
        job = MagicMock()
        job.get = lambda k, d=None: {
            "ClusterId": 100, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 16, "RequestMemory": 65536,
            "MemoryUsage": 0, "ImageSize": 1, "ResidentSetSize": 0,
            "MemoryProvisioned": 65536, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_5@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        job.eval = lambda k: job.get(k)

        _mem, cpu = self._run(job)
        self.assertAlmostEqual(cpu, 0.518, places=3)

    def test_does_set_memory_when_schedd_has_real_rss(self):
        # When the schedd DOES have real data, the personal loop should still
        # write it (the startd fallback only fills in when schedd is silent).
        job = MagicMock()
        job.get = lambda k, d=None: {
            "ClusterId": 100, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 4, "RequestMemory": 8192,
            "MemoryUsage": 0, "ImageSize": 0, "ResidentSetSize": 10240,
            "MemoryProvisioned": 0, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_1@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        job.eval = lambda k: job.get(k)

        mem, _cpu = self._run(job)
        # 10240 KB / 1024 = 10 MB
        self.assertAlmostEqual(mem, 10.0, places=3)

    def test_does_set_cpu_efficiency_when_schedd_has_real_cpu(self):
        # When the schedd has real CPU data, the personal loop should use it.
        job = MagicMock()
        job.get = lambda k, d=None: {
            "ClusterId": 100, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 2, "RequestMemory": 8192,
            "MemoryUsage": 0, "ImageSize": 0, "ResidentSetSize": 0,
            "MemoryProvisioned": 0, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 30.0, "RemoteSysCpu": 5.0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_1@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        job.eval = lambda k: job.get(k)

        _mem, cpu = self._run(job)
        # 30 + 5 = 35 cpu-seconds used. duration = now - JobStartDate = ~0
        # in tests (both are t0), so efficiency is ~0 — what matters is that
        # the metric was touched (i.e. set, not left at the startd fallback
        # value of 0.518).
        # We assert: cpu was not left at 0.518 (the startd fallback value).
        self.assertNotAlmostEqual(cpu, 0.518, places=3)


class TestJobStatusEmittedInPersonalLoop(unittest.TestCase):
    """scrape_personal must emit stoomboot_job_status for running + queued jobs.

    The bug: the personal loop cleared the metric via _PERSONAL_GAUGES but never
    re-emitted it, so any per-user panel referencing it returned 0 series.
    """

    def _run_scrape_personal(self):
        """Run scrape_personal against a fake collector and return the metric series."""
        from stoomboot_gpu_exporter import scrape_personal, job_status_gauge

        # Build a fake schedd ad
        schedd_ad = MagicMock()
        schedd_ad.get = lambda k, d=None: {
            "Name": "schedd-fake", "MyAddress": "x", "CondorVersion": "10.0"
        }.get(k, d)

        # Two jobs: one running, one queued
        running_job = MagicMock()
        running_job.get = lambda k, d=None: {
            "ClusterId": 123, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 16, "RequestMemory": 65536,
            "MemoryUsage": 0, "ImageSize": 2, "ResidentSetSize": 0,
            "MemoryProvisioned": 65536, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_5@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        running_job.eval = lambda k: running_job.get(k)

        queued_job = MagicMock()
        queued_job.get = lambda k, d=None: {
            "ClusterId": 124, "ProcId": 0, "JobStatus": 1,
            "RequestGPUs": 0, "RequestCpus": 4, "RequestMemory": 8192,
            "MemoryUsage": 0, "ImageSize": 0, "ResidentSetSize": 0,
            "MemoryProvisioned": 0, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 0, "RemoteHost": "", "LastRemoteHost": "",
            "AssignedGPUs": "",
        }.get(k, d)
        queued_job.eval = lambda k: queued_job.get(k)

        schedd = MagicMock()
        schedd.query = MagicMock(return_value=[running_job, queued_job])

        coll = MagicMock()
        coll.query = MagicMock(return_value=[schedd_ad])

        # Patch the Collector to return our fake
        with unittest.mock.patch("stoomboot_gpu_exporter.htcondor") as ht_mock:
            ht_mock.Collector = MagicMock(return_value=coll)
            ht_mock.Schedd = MagicMock(return_value=schedd)
            ht_mock.AdTypes = _htcondor.AdTypes
            scrape_personal("fake-collector", "testuser")

        return job_status_gauge

    def test_status_emitted_for_running_job(self):
        g = self._run_scrape_personal()
        value = g.labels(cluster="cpu", user="testuser", job_id="123.0")._value.get()
        self.assertEqual(value, 2)

    def test_status_emitted_for_queued_job(self):
        g = self._run_scrape_personal()
        value = g.labels(cluster="cpu", user="testuser", job_id="124.0")._value.get()
        self.assertEqual(value, 1)


class TestStartdCacheInvalidatedOnNewJob(unittest.TestCase):
    """When scrape_personal discovers a job the startd fallback hasn't seen
    yet, the cached collector result must be invalidated so the next
    _fetch_htcondor_gpu_metrics call re-queries — otherwise the new job's
    memory metric is stuck on stale data for up to _HTCONDOR_GPU_CACHE_TTL
    seconds. That delay is the "first poll at the start of every new run"
    annoyance."""

    def setUp(self):
        import stoomboot_gpu_exporter as exp
        # Force the cache to look fresh — anything short of an explicit
        # invalidation should leave it untouched.
        exp._htcondor_gpu_cache = {"stale_job": {"memory_usage_mb": 999}}
        exp._htcondor_gpu_cache_time = time.time()
        # Start the personal loop with no known jobs
        exp._personal_mem_labels = set()
        exp._personal_cpu_labels = set()

    def _make_running_job(self, cluster_id):
        job = MagicMock()
        job.get = lambda k, d=None: {
            "ClusterId": cluster_id, "ProcId": 0, "JobStatus": 2,
            "RequestGPUs": 0, "RequestCpus": 4, "RequestMemory": 8192,
            "MemoryUsage": 0, "ImageSize": 0, "ResidentSetSize": 0,
            "MemoryProvisioned": 0, "TotalJobRunningCpuUsage": 0,
            "RemoteUserCpu": 0, "RemoteSysCpu": 0,
            "JobStartDate": 1.0, "RemoteHost": "slot1_1@wn-lot-007.nikhef.nl",
            "LastRemoteHost": "", "AssignedGPUs": "",
        }.get(k, d)
        job.eval = lambda k: job.get(k)
        return job

    def _run_personal(self, jobs):
        from stoomboot_gpu_exporter import scrape_personal
        import stoomboot_gpu_exporter as exp

        schedd_ad = MagicMock()
        schedd_ad.get = lambda k, d=None: {
            "Name": "schedd-fake", "MyAddress": "x", "CondorVersion": "10.0"
        }.get(k, d)

        schedd = MagicMock()
        schedd.query = MagicMock(return_value=jobs)

        coll = MagicMock()
        coll.query = MagicMock(return_value=[schedd_ad])

        with unittest.mock.patch("stoomboot_gpu_exporter.htcondor") as ht_mock:
            ht_mock.Collector = MagicMock(return_value=coll)
            ht_mock.Schedd = MagicMock(return_value=schedd)
            ht_mock.AdTypes = _htcondor.AdTypes
            scrape_personal("fake-collector", "testuser")

    def test_cache_invalidated_when_new_job_appears(self):
        # First pass: job 200 is running. Cache is invalidated (no prior jobs).
        self._run_personal([self._make_running_job(200)])
        import stoomboot_gpu_exporter as exp

        # Re-arm the cache to look fresh
        exp._htcondor_gpu_cache = {"stale": {"memory_usage_mb": 1}}
        exp._htcondor_gpu_cache_time = time.time()
        fresh_time = exp._htcondor_gpu_cache_time

        # Second pass: job 201 (new!) appears. Cache must be invalidated.
        self._run_personal([self._make_running_job(201)])

        self.assertEqual(exp._htcondor_gpu_cache_time, 0,
                         "Cache should be invalidated when a new job is detected")
        self.assertEqual(exp._htcondor_gpu_cache, {})

    def test_cache_not_invalidated_for_repeat_job(self):
        # First pass: job 300. Cache invalidated.
        self._run_personal([self._make_running_job(300)])
        import stoomboot_gpu_exporter as exp

        # Re-arm cache
        exp._htcondor_gpu_cache = {"300.0": {"memory_usage_mb": 512}}
        exp._htcondor_gpu_cache_time = time.time()
        fresh_time = exp._htcondor_gpu_cache_time

        # Second pass: same job 300 (no new jobs). Cache must NOT be invalidated.
        self._run_personal([self._make_running_job(300)])

        self.assertEqual(exp._htcondor_gpu_cache_time, fresh_time,
                         "Cache should not be invalidated when no new jobs appear")
        self.assertEqual(exp._htcondor_gpu_cache, {"300.0": {"memory_usage_mb": 512}})


class TestFetchSshMemoryBatch(unittest.TestCase):
    """_fetch_ssh_memory_batch SSHes to a worker node, reads
    /proc/<pid>/status for one or more PIDs, and returns {pid: mem_mb}.
    This is the real-time RSS path that bypasses the startd's slow
    1-5 minute update interval."""

    def setUp(self):
        # Reset throttle state and enable SSH path (empty _ssh_user short-circuits)
        import stoomboot_gpu_exporter as exp
        exp._last_ssh_poll = {}
        exp._ssh_user = "testuser"

    def _mock_ssh(self, stdout="", returncode=0, side_effect=None):
        """Patch subprocess.run to return a fake ssh result."""
        import subprocess
        import stoomboot_gpu_exporter as exp
        result = unittest.mock.MagicMock()
        result.stdout = stdout
        result.stderr = ""
        result.returncode = returncode
        if side_effect is not None:
            run = unittest.mock.MagicMock(side_effect=side_effect)
        else:
            run = unittest.mock.MagicMock(return_value=result)
        patcher = unittest.mock.patch.object(exp.subprocess, "run", run)
        patcher.start()
        self.addCleanup(patcher.stop)
        return run

    def test_parses_vmrss_for_single_pid(self):
        # /proc/PID/status output — VmRSS is in kB
        proc_status = (
            "Name:	python3\n"
            "VmSize:	   12345 kB\n"
            "VmRSS:	   4096 kB\n"
            "VmData:	    512 kB\n"
        )
        # Format: each PID wrapped in ==<pid>== delimiter markers
        ssh_output = f"==100==\n{proc_status}"
        self._mock_ssh(stdout=ssh_output)
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [100])
        self.assertAlmostEqual(out[100], 4.0, places=3)  # 4096 kB = 4 MB

    def test_parses_vmrss_for_multiple_pids(self):
        block_a = "==100==\nVmRSS:	   2048 kB\n"
        block_b = "==200==\nVmRSS:	  10240 kB\n"
        self._mock_ssh(stdout=block_a + block_b)
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [100, 200])
        self.assertAlmostEqual(out[100], 2.0, places=3)
        self.assertAlmostEqual(out[200], 10.0, places=3)

    def test_skips_pid_whose_proc_status_is_missing(self):
        # PID 200's `cat /proc/200/status` returned nothing (job died, wrong
        # node, or PID reused). The function should silently skip it.
        block_a = "==100==\nVmRSS:	   8192 kB\n"
        # No block for PID 200
        self._mock_ssh(stdout=block_a)
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [100, 200])
        self.assertIn(100, out)
        self.assertNotIn(200, out)

    def test_returns_empty_dict_on_empty_pid_list(self):
        run = self._mock_ssh()
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [])
        self.assertEqual(out, {})
        run.assert_not_called()  # Don't even attempt SSH

    def test_returns_empty_dict_on_ssh_timeout(self):
        import subprocess
        self._mock_ssh(side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=3))
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [100])
        self.assertEqual(out, {})

    def test_returns_empty_dict_on_nonzero_returncode(self):
        # SSH returns 255 when auth fails or host unreachable
        self._mock_ssh(stdout="", returncode=255)
        from stoomboot_gpu_exporter import _fetch_ssh_memory_batch
        out = _fetch_ssh_memory_batch("wn-lot-007", [100])
        self.assertEqual(out, {})


class TestSshPollThrottle(unittest.TestCase):
    """_should_ssh_poll gates SSH calls so we don't SSH the same job on
    every 3s scrape. Default interval: 10s."""

    def setUp(self):
        import stoomboot_gpu_exporter as exp
        exp._last_ssh_poll = {}
        exp._ssh_poll_interval = 10.0
        exp._ssh_user = "testuser"

    def test_returns_true_for_new_job(self):
        from stoomboot_gpu_exporter import _should_ssh_poll
        self.assertTrue(_should_ssh_poll("100.0", "wn-lot-007"))

    def test_returns_false_within_throttle_window(self):
        from stoomboot_gpu_exporter import _should_ssh_poll, _mark_ssh_polled
        _mark_ssh_polled("100.0", "wn-lot-007")
        self.assertFalse(_should_ssh_poll("100.0", "wn-lot-007"))

    def test_returns_true_after_throttle_window_elapses(self):
        import time
        from stoomboot_gpu_exporter import _should_ssh_poll, _mark_ssh_polled
        _mark_ssh_polled("100.0", "wn-lot-007")
        # Pretend 11s passed
        import stoomboot_gpu_exporter as exp
        key = ("100.0", "wn-lot-007")
        exp._last_ssh_poll[key] = exp._last_ssh_poll[key] - 11.0
        self.assertTrue(_should_ssh_poll("100.0", "wn-lot-007"))

    def test_throttle_is_per_job(self):
        # Polling job 100 doesn't throttle job 200
        from stoomboot_gpu_exporter import _should_ssh_poll, _mark_ssh_polled
        _mark_ssh_polled("100.0", "wn-lot-007")
        self.assertFalse(_should_ssh_poll("100.0", "wn-lot-007"))
        self.assertTrue(_should_ssh_poll("200.0", "wn-lot-007"))


class TestScrapeWorkerNodesSshOverrides(unittest.TestCase):
    """_scrape_worker_nodes must use SSH /proc reads as a fresher source than
    the startd ClassAd, when a job has a PID and SSH succeeds. The startd
    updates RSS / MemoryUsage only every 1-5 minutes, but /proc is real-time.
    Without SSH, the user sees stale RAM for up to several minutes after a
    job starts; with SSH, RAM updates within the throttle window (default 10s)."""

    def setUp(self):
        import stoomboot_gpu_exporter as exp
        exp._htcondor_gpu_cache = {
            "100.0": {"memory_usage_mb": 999.0, "pid": 1234},  # startd's stale value
        }
        exp._htcondor_gpu_cache_time = time.time()  # fresh, so cached value is returned
        exp._last_ssh_poll = {}
        exp._ssh_poll_interval = 10.0
        exp._ssh_user = "testuser"
        # Clear the metric so we can read fresh values
        from prometheus_client import REGISTRY
        # Best-effort: just call .clear() on the gauge if supported; otherwise skip
        try:
            exp.job_memory_usage_mb._metrics.clear()
        except Exception:
            pass

    def _run_with_ssh_output(self, ssh_stdout, ssh_returncode=0,
                             ssh_side_effect=None):
        import subprocess
        import unittest.mock
        import stoomboot_gpu_exporter as exp
        from stoomboot_gpu_exporter import _scrape_worker_nodes

        # Mock subprocess.run for the SSH call
        if ssh_side_effect is not None:
            run = unittest.mock.MagicMock(side_effect=ssh_side_effect)
        else:
            result = unittest.mock.MagicMock()
            result.stdout = ssh_stdout
            result.stderr = ""
            result.returncode = ssh_returncode
            run = unittest.mock.MagicMock(return_value=result)

        # Mock the HTCondor collector so _fetch_htcondor_gpu_metrics returns
        # the pre-seeded cache from setUp.
        coll = unittest.mock.MagicMock()
        coll.query = unittest.mock.MagicMock(return_value=[])

        with unittest.mock.patch.object(exp.subprocess, "run", run), \
             unittest.mock.patch.object(exp.htcondor, "Collector", return_value=coll):
            job = dict(job_id="100.0", node="wn-lot-007", gpu_uuid="",
                       req_cpus=4, cluster="cpu", user="testuser")
            _scrape_worker_nodes([job], time.time())
        return run

    def test_ssh_value_overrides_startd_value(self):
        # Startd says 999 MB (stale). SSH says 8 MB (real-time).
        ssh_out = "==1234==\nVmRSS:	   8192 kB\n"
        self._run_with_ssh_output(ssh_out)
        from stoomboot_gpu_exporter import job_memory_usage_mb
        labels = dict(cluster="cpu", user="testuser", job_id="100.0",
                      node="wn-lot-007", resource_type="CPU")
        # 8192 kB = 8 MB
        v = job_memory_usage_mb.labels(**labels)._value.get()
        self.assertAlmostEqual(v, 8.0, places=2)

    def test_startd_value_kept_when_ssh_fails(self):
        # SSH times out — fall back to startd's value
        import subprocess
        self._run_with_ssh_output(
            ssh_stdout="", ssh_side_effect=subprocess.TimeoutExpired(cmd="ssh", timeout=3)
        )
        from stoomboot_gpu_exporter import job_memory_usage_mb
        labels = dict(cluster="cpu", user="testuser", job_id="100.0",
                      node="wn-lot-007", resource_type="CPU")
        v = job_memory_usage_mb.labels(**labels)._value.get()
        self.assertAlmostEqual(v, 999.0, places=2)

    def test_ssh_not_called_within_throttle_window(self):
        import unittest.mock
        import stoomboot_gpu_exporter as exp
        from stoomboot_gpu_exporter import _scrape_worker_nodes, _mark_ssh_polled
        # Mark job as just polled
        _mark_ssh_polled("100.0", "wn-lot-007")
        run = self._run_with_ssh_output(ssh_stdout="==1234==\nVmRSS:	   8192 kB\n")
        # subprocess.run should NOT have been called — we're throttled
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
