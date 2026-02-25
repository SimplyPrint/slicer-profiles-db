"""Progress reporting for slicer pipeline operations."""

from __future__ import annotations

from typing import Protocol


class DownloadProgress(Protocol):
    """Progress tracker for a single download."""

    def update(self, bytes_downloaded: int) -> None: ...
    def close(self) -> None: ...


class ProgressReporter(Protocol):
    """Protocol for pipeline progress reporting."""

    def update_status(self, message: str) -> None: ...
    def create_download_bar(self, total_bytes: int, description: str) -> DownloadProgress: ...
    def step(self, step_name: str, current: int, total: int) -> None: ...


class RichProgressReporter:
    """Rich-based progress reporter with download bars and status messages."""

    def __init__(self) -> None:
        from rich.console import Console

        self.console = Console()

    def update_status(self, message: str) -> None:
        self.console.print(f"[bold blue]>>>[/] {message}")

    def create_download_bar(self, total_bytes: int, description: str) -> RichDownloadProgress:
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=self.console,
        )
        progress.start()
        task_id = progress.add_task(description, total=total_bytes or None)
        return RichDownloadProgress(progress, task_id)

    def step(self, step_name: str, current: int, total: int) -> None:
        self.console.print(f"  [dim]\\[{current}/{total}][/] {step_name}")


class RichDownloadProgress:
    """Wraps a rich Progress bar for a single download."""

    def __init__(self, progress, task_id) -> None:  # type: ignore[no-untyped-def]
        self._progress = progress
        self._task_id = task_id

    def update(self, bytes_downloaded: int) -> None:
        self._progress.update(self._task_id, advance=bytes_downloaded)

    def close(self) -> None:
        self._progress.stop()


class NullProgressReporter:
    """No-op reporter for --json mode or testing."""

    def update_status(self, message: str) -> None:
        pass

    def create_download_bar(self, total_bytes: int, description: str) -> NullDownloadProgress:
        return NullDownloadProgress()

    def step(self, step_name: str, current: int, total: int) -> None:
        pass


class NullDownloadProgress:
    """No-op download progress tracker."""

    def update(self, bytes_downloaded: int) -> None:
        pass

    def close(self) -> None:
        pass
