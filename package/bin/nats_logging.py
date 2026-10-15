# Copyright 2025 Brett Adams
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Logging for the add-on's inputs and search commands.

Each input and search command process writes everything it logs (its own
modules, nats-py and asyncio included) to its own file in
$SPLUNK_HOME/var/log/splunk, which Splunk indexes into _internal:
`nats_subscribe_<input>.log`, `nats_kv_<input>.log`, `nats_natssubscribe.log`
and `nats_natskv.log`. Every record is one line, so nothing reaches splunkd.log
through stderr and a traceback never becomes one splunkd.log ERROR per line.

The level comes from the Logging tab (nats_settings.conf [logging] loglevel).
Inputs run until Splunk stops them, so they re-read it periodically and a change
takes effect without restarting them.

The UCC Monitoring Dashboard reads two kinds of records from these files:
- `action=events_ingested` records (solnlib.log.events_ingested), counted per
  input and sourcetype and written at most once per HOUSEKEEPING_INTERVAL. They
  are dashboard data rather than diagnostics, so they are written whatever the
  Logging level.
- ERROR records with an `exc_l="<error type>"` label, grouped by that label.
Its license usage panels match events to inputs by source (see event_source).
"""

import asyncio
import logging
import time
from collections import Counter
from typing import Callable, Optional

from solnlib import conf_manager, log

APP_NAME = "nats"
SETTINGS_CONF = "nats_settings"

# Seconds between ingestion counts and between re-reads of the Logging level
HOUSEKEEPING_INTERVAL = 60

# Records the dashboard needs are logged here: this logger stays at INFO and
# propagates to the root logger's handler whatever the root level is.
monitoring_logger = logging.getLogger("nats.monitoring")
monitoring_logger.setLevel(logging.INFO)

_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class OneLineFormatter(logging.Formatter):
    """A formatter that keeps every record on one line, tracebacks included."""

    def format(self, record: logging.LogRecord) -> str:
        return super().format(record).replace("\r\n", "\\n").replace("\n", "\\n")


def parse_level(value: Optional[str], default: int = logging.INFO) -> int:
    """The logging level for a Logging tab value such as 'DEBUG' or 'WARN'."""
    return _LEVELS.get(str(value or "").strip().upper(), default)


def read_level(session_key: str) -> int:
    """Read the Logging level set on the add-on's Configuration page."""
    value = conf_manager.get_log_level(
        logger=logging.getLogger(__name__),
        session_key=session_key,
        app_name=APP_NAME,
        conf_name=SETTINGS_CONF,
    )
    return parse_level(value)


def setup(name: str, session_key: Optional[str]) -> logging.Logger:
    """
    Send everything this process logs to $SPLUNK_HOME/var/log/splunk/<name>.log at
    the Logging level, and return the root logger.

    Raises:
        Exception: If the log file cannot be opened
    """
    file_logger = log.Logs().get_logger(name)
    root = logging.getLogger()
    for handler in file_logger.handlers:
        handler.setFormatter(OneLineFormatter(log.Logs._default_log_format))
        if handler not in root.handlers:
            root.addHandler(handler)
    # Python warnings would otherwise go to stderr.
    logging.captureWarnings(True)
    level = logging.INFO
    if session_key:
        try:
            level = read_level(session_key)
        except Exception as e:
            root.warning("Failed to read the Logging level, using INFO: %s", e)
    set_level(level)
    return root


def set_level(level: int) -> bool:
    """Apply a Logging level; returns whether it changed."""
    root = logging.getLogger()
    if root.level == level:
        return False
    root.setLevel(level)
    return True


def setup_input(input_name: str, session_key: Optional[str]) -> logging.Logger:
    """Set up logging for an input named like nats_subscribe://<name>."""
    kind, _, name = input_name.partition("://")
    return setup(f"{kind}_{name}", session_key)


def setup_command(
    command: str, session_key: Optional[str], splunklib_logger: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging for a search command such as natssubscribe. Records of the
    command's splunklib logger (named `splunklib_logger`) stay in search.log: it
    repeats every error the command reports, without the error type.
    """
    root = setup(f"{APP_NAME}_{command}", session_key)
    if splunklib_logger:
        for handler in log.Logs().get_logger(f"{APP_NAME}_{command}").handlers:
            handler.addFilter(lambda record: record.name != splunklib_logger)
    return root


def log_error(
    logger: logging.Logger, label: str, message: str, *args, **kwargs
) -> None:
    """
    Log an error in the form the Monitoring Dashboard groups by: `label` is the
    error type it shows, e.g. "Connection Error".
    """
    # Record the caller's file, function and line rather than this function's
    kwargs.setdefault("stacklevel", 2)
    logger.error(f'exc_l="{label}" {message}', *args, **kwargs)


def event_source(input_name: str, subject: str) -> str:
    """
    The source an input writes an event with: its input name, then the message's
    NATS subject. At index time, transforms.conf [nats_subject_from_source] moves
    the subject into the indexed field nats_subject, so events are indexed with
    the input name as their source: the Monitoring Dashboard matches license usage
    to inputs by source. NATS subjects never contain spaces.
    """
    return f"{input_name} {subject}"


class Ingestion:
    """
    Counts the events an input writes, and logs them for the Monitoring Dashboard
    as one events_ingested record per sourcetype when flushed.
    """

    def __init__(self, input_name: str, index: Optional[str], account: Optional[str]):
        self.input_name = input_name
        # Events of an input without an index setting go to the default index.
        self.index = index if index and index != "default" else "main"
        self.account = account
        self._counts: Counter = Counter()

    def add(self, sourcetype: str, count: int = 1) -> None:
        self._counts[sourcetype] += count

    def flush(self) -> None:
        for sourcetype, count in self._counts.items():
            log.events_ingested(
                monitoring_logger,
                self.input_name,
                sourcetype,
                count,
                self.index,
                account=self.account,
            )
        self._counts.clear()


class InputHousekeeping:
    """
    The periodic logging work of a running input: flushing its ingestion counts
    and picking up a changed Logging level.
    """

    def __init__(
        self,
        ingestion: Ingestion,
        session_key: Optional[str],
        read: Callable[[str], int] = read_level,
        interval: float = HOUSEKEEPING_INTERVAL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ingestion = ingestion
        self._session_key = session_key
        self._read = read
        self.interval = interval
        self._clock = clock
        self._last_level_read = clock()

    async def run(self, stop: asyncio.Event) -> None:
        """Flush counts every interval until `stop` is set, then flush once more."""
        try:
            while not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
                await self.tick()
        finally:
            self.ingestion.flush()

    async def tick(self) -> None:
        self.ingestion.flush()
        if not self._session_key or self._clock() - self._last_level_read < self.interval:
            return
        self._last_level_read = self._clock()
        try:
            # A REST call to splunkd; keep it off the event loop.
            loop = asyncio.get_running_loop()
            level = await loop.run_in_executor(None, self._read, self._session_key)
        except Exception as e:
            logging.getLogger(__name__).warning("Failed to read the Logging level: %s", e)
            return
        if set_level(level):
            monitoring_logger.info(
                "Logging level changed to %s", logging.getLevelName(level)
            )
