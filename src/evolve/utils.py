import functools
import sys
import os
import psutil
import threading
from datetime import datetime


def monitor_usage(
    interval: float | int = 0.5,
    logfile: str | None = None,
    normalize: bool = False,
    network: bool = True,
    disk: bool = True,
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            proc = psutil.Process(os.getpid())
            stop_event = threading.Event()
            logical_cpus = psutil.cpu_count(logical=True)

            if logical_cpus is None:
                raise ValueError("could not find how many logical cpus!")

            if logfile:
                f = open(logfile, "a")
            else:
                f = None

            # Initial counters
            prev_net = (
                proc.net_io_counters()
                if network and hasattr(proc, "net_io_counters")
                else None
            )
            prev_io = proc.io_counters() if disk else None

            def log_usage():
                nonlocal prev_net, prev_io
                while not stop_event.is_set():
                    # Percentage since last call
                    cpu = proc.cpu_percent(interval=None)
                    if normalize:
                        cpu = cpu / logical_cpus  # normalize to 0-100%
                    mem = proc.memory_info().rss / (1024 * 1024)  # MB
                    net_info = ""
                    if network and prev_net is not None:
                        new_net = proc.net_io_counters()
                        sent_diff = new_net.bytes_sent - prev_net.bytes_sent
                        recv_diff = new_net.bytes_recv - prev_net.bytes_recv
                        prev_net = new_net
                        net_info = f"\t| NET: +{sent_diff / 1024:.1f}KB sent,\t+{recv_diff / 1024:.1f}KB recv"

                    disk_info = ""
                    if disk and prev_io is not None:
                        new_io = proc.io_counters()
                        read_diff = new_io.read_bytes - prev_io.read_bytes
                        write_diff = new_io.write_bytes - prev_io.write_bytes
                        prev_io = new_io
                        disk_info = f"\t| DISK: +{read_diff / 1024:.1f}KB read,\t+{write_diff / 1024:.1f}KB written"

                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    line = f"{now}\t[USAGE]  CPU:\t{cpu:.1f}%\t| RAM:\t{mem:.1f} MB{net_info}{disk_info}\n"
                    if f:
                        f.write(line)
                        f.flush()
                    else:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    if stop_event.wait(interval):
                        break

            # pre-call to cpu_percent to prime it
            proc.cpu_percent(interval=None)

            t = threading.Thread(target=log_usage, daemon=True)
            t.start()

            try:
                result = func(*args, **kwargs)
            finally:
                stop_event.set()
                t.join()
                if f:
                    f.close()
            return result

        return wrapper

    return decorator
