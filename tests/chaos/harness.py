import logging
import subprocess
import sys
import time
from typing import Self

import psutil

logger = logging.getLogger(__name__)


class WorkerProcess:
    """
    Subprocess-based worker harness that allows testing actual OS-level
    process signals: SIGKILL (kill), SIGSTOP (suspend), and SIGCONT (resume).
    """

    def __init__(
        self,
        worker_id: str,
        db_url: str,
        lease_ttl: int = 5,
        concurrency: int = 1,
        poll_interval: float = 0.1,
    ) -> None:
        self.worker_id = worker_id
        self.db_url = db_url
        self.lease_ttl = lease_ttl
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self._proc: subprocess.Popen[str] | None = None
        self._psutil_proc: psutil.Process | None = None

    def start(self) -> None:
        """Launches the worker in a distinct OS process."""
        if self._proc is not None:
            raise RuntimeError(f"WorkerProcess '{self.worker_id}' is already running")

        cmd = [
            sys.executable,
            "-m",
            "app.worker.runner",
            "--worker-id",
            self.worker_id,
            "--db-url",
            self.db_url,
            "--lease-ttl",
            str(self.lease_ttl),
            "--concurrency",
            str(self.concurrency),
            "--poll-interval",
            str(self.poll_interval),
        ]

        logger.info("Spawning WorkerProcess '%s': %s", self.worker_id, " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._psutil_proc = psutil.Process(self._proc.pid)
        # Give the process a brief moment to boot and connect
        time.sleep(0.3)

    @property
    def pid(self) -> int:
        if self._proc is None:
            raise RuntimeError("Process is not running")
        return self._proc.pid

    def is_alive(self) -> bool:
        """Returns True if the process is currently running."""
        if self._proc is None:
            return False
        return self._proc.poll() is None

    def suspend(self) -> None:
        """
        Pauses the process completely (SIGSTOP equivalent).
        Freezes event loop, preventing heartbeats from being sent.
        Recursively suspends child processes (needed on Windows where python.exe is a shim).
        """
        if self._psutil_proc and self.is_alive():
            logger.warning(
                "Suspending (SIGSTOP) WorkerProcess '%s' (pid=%d)", self.worker_id, self.pid
            )
            try:
                for child in self._psutil_proc.children(recursive=True):
                    try:
                        child.suspend()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                self._psutil_proc.suspend()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def resume(self) -> None:
        """
        Resumes the previously paused process (SIGCONT equivalent).
        Allows frozen worker to wake up and attempt commit as a zombie.
        Recursively resumes child processes.
        """
        if self._psutil_proc and self.is_alive():
            logger.warning(
                "Resuming (SIGCONT) WorkerProcess '%s' (pid=%d)", self.worker_id, self.pid
            )
            try:
                self._psutil_proc.resume()
                for child in self._psutil_proc.children(recursive=True):
                    try:
                        child.resume()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def kill(self) -> None:
        """Forcefully kills the process tree immediately (SIGKILL equivalent)."""
        if self._proc and self.is_alive():
            logger.warning(
                "Killing (SIGKILL) WorkerProcess '%s' (pid=%d)", self.worker_id, self.pid
            )
            if self._psutil_proc:
                try:
                    for child in self._psutil_proc.children(recursive=True):
                        try:
                            child.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    self._psutil_proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            else:
                self._proc.kill()
            try:
                self._proc.wait(timeout=3.0)
            except Exception:
                pass

    def stop(self) -> None:
        """Gracefully terminates worker process and cleans up."""
        if self._proc and self.is_alive():
            try:
                if self._psutil_proc:
                    for child in self._psutil_proc.children(recursive=True):
                        try:
                            child.terminate()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                self._proc.terminate()
                self._proc.wait(timeout=2.0)
            except Exception:
                self.kill()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.stop()
