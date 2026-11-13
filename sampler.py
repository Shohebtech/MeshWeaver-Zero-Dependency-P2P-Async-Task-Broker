"""Measure this node's own CPU and RAM usage (percent, 0-100).

Pure standard library: /proc on Linux, ctypes on Windows, load-average
fallback elsewhere. If `psutil` happens to be installed it is used instead,
but it is never required.
"""
import os
import sys


class LoadSampler:
    def __init__(self):
        self._psutil = None
        try:  # optional, never required
            import psutil  # type: ignore
            self._psutil = psutil
            psutil.cpu_percent(None)
        except Exception:
            pass
        self._prev = self._read_cpu_times()

    # ---------- public ----------
    def sample(self):
        """Return (cpu_percent, ram_percent)."""
        return self.cpu_percent(), self.ram_percent()

    def cpu_percent(self) -> float:
        if self._psutil:
            return float(self._psutil.cpu_percent(None))
        cur = self._read_cpu_times()
        if cur is None or self._prev is None:
            return self._loadavg_cpu()
        idle_d = cur[0] - self._prev[0]
        total_d = cur[1] - self._prev[1]
        self._prev = cur
        if total_d <= 0:
            return 0.0
        return max(0.0, min(100.0, 100.0 * (1.0 - idle_d / total_d)))

    def ram_percent(self) -> float:
        if self._psutil:
            return float(self._psutil.virtual_memory().percent)
        try:
            if sys.platform.startswith("linux"):
                return self._ram_linux()
            if sys.platform == "win32":
                return self._ram_windows()
        except Exception:
            pass
        return 0.0

    # ---------- CPU internals ----------
    def _read_cpu_times(self):
        """Return (idle, total) counters, or None if unsupported."""
        try:
            if sys.platform.startswith("linux"):
                with open("/proc/stat") as f:
                    parts = f.readline().split()[1:]
                vals = [int(x) for x in parts]
                idle = vals[3] + (vals[4] if len(vals) > 4 else 0)  # idle + iowait
                return idle, sum(vals)
            if sys.platform == "win32":
                import ctypes
                from ctypes import wintypes

                class FILETIME(ctypes.Structure):
                    _fields_ = [("lo", wintypes.DWORD), ("hi", wintypes.DWORD)]

                idle, kern, user = FILETIME(), FILETIME(), FILETIME()
                ctypes.windll.kernel32.GetSystemTimes(
                    ctypes.byref(idle), ctypes.byref(kern), ctypes.byref(user))
                to_int = lambda ft: (ft.hi << 32) | ft.lo
                return to_int(idle), to_int(kern) + to_int(user)  # kernel includes idle
        except Exception:
            pass
        return None

    @staticmethod
    def _loadavg_cpu() -> float:
        try:
            return max(0.0, min(100.0, 100.0 * os.getloadavg()[0] / (os.cpu_count() or 1)))
        except (AttributeError, OSError):
            return 0.0

    # ---------- RAM internals ----------
    @staticmethod
    def _ram_linux() -> float:
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                info[k] = int(v.split()[0])
        total, avail = info["MemTotal"], info.get("MemAvailable", info.get("MemFree", 0))
        return 100.0 * (total - avail) / total

    @staticmethod
    def _ram_windows() -> float:
        import ctypes

        class MEMSTAT(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]

        st = MEMSTAT()
        st.dwLength = ctypes.sizeof(MEMSTAT)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
        return float(st.dwMemoryLoad)