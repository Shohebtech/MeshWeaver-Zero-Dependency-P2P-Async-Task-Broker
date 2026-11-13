import unittest

from modules.gossip import load_monitor as lm

STAT_A = "cpu  100 0 100 800 0 0 0 0 0 0\ncpu0 1 2 3 4"
STAT_B = "cpu  200 0 200 1000 0 0 0 0 0 0\n"
MEM = "MemTotal:       1000 kB\nMemFree:         100 kB\nMemAvailable:    250 kB\n"


class LoadMonitorTests(unittest.TestCase):
    def test_parse_proc_stat(self):
        self.assertEqual(lm.parse_proc_stat(STAT_A), (200, 1000))
        self.assertIsNone(lm.parse_proc_stat("garbage"))

    def test_cpu_delta_math(self):
        a, b = lm.parse_proc_stat(STAT_A), lm.parse_proc_stat(STAT_B)
        busy = (b[0] - a[0]) * 100.0 / (b[1] - a[1])
        self.assertAlmostEqual(busy, 50.0)  # +200 busy of +400 total

    def test_parse_meminfo(self):
        self.assertAlmostEqual(lm.parse_meminfo(MEM), 75.0)
        self.assertIsNone(lm.parse_meminfo("nothing"))

    def test_sample_in_range_for_every_backend(self):
        for backend in ("auto", "proc", "loadavg"):
            with self.subTest(backend=backend):
                try:
                    mon = lm.LoadMonitor(backend)
                except OSError:
                    self.skipTest("backend unavailable on this OS")
                s = mon.sample()
                self.assertTrue(0.0 <= s.cpu <= 100.0)
                self.assertTrue(0.0 <= s.ram <= 100.0)


if __name__ == "__main__":
    unittest.main()
