#!/usr/bin/env python3
"""
Reads raw VE.Direct text protocol data directly from serial port to extract current measurements from a shunt before D-Bus processing by Venus OS.
"""

import logging
import serial
import termios
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict

SHUNT_BAUD_RATE = 19200
SERIAL_TIMEOUT_SECONDS = 0.5
DATA_EXPIRATION_SECONDS = 30


class VeKey(Enum):
    CHECKSUM = "Checksum"
    CHECKSUM_VALID = "_checksum_valid"
    CONSUMED_MAH = "CE"
    CURRENT_MA = "I"
    SOC_PERMILLE = "SOC"


@dataclass
class VeDirectShuntData:
    consumed_ah: float
    current_amps: float
    soc_percent: float
    read_timestamp: float


class VeDirectParser:
    def __init__(self):
        self.buffer = b""
        self.frame_data: Dict[str, str] = {}
        self.checksum = 0

    def feed(self, data: bytes) -> None:
        """Feed raw bytes into parser."""
        self.buffer += data
        # Prevent buffer overflow
        if len(self.buffer) > 8192:
            self.buffer = self.buffer[-4096:]
            self.frame_data = {}
            self.checksum = 0

    def next_frame(self) -> Optional[Dict[str, str]]:
        """Returns complete frame dict when a full frame is received, None otherwise."""
        while b"\n" in self.buffer:
            newline_pos = self.buffer.index(b"\n")
            line = self.buffer[:newline_pos]
            self.checksum += sum(self.buffer[: newline_pos + 1])
            self.buffer = self.buffer[newline_pos + 1 :]

            if not line:
                continue

            try:
                line_str = line.decode("ascii", errors="replace")
            except Exception:
                continue

            # Parse TAB-separated key-value pair
            if "\t" in line_str:
                parts = line_str.split("\t", 1)
                if len(parts) == 2:
                    key, value = parts[0].strip(), parts[1].strip()
                    self.frame_data[key] = value

                    # Checksum marks end of frame
                    if key == VeKey.CHECKSUM.value:
                        frame = self.frame_data.copy()
                        frame[VeKey.CHECKSUM_VALID.value] = self._verify_checksum()
                        self.frame_data = {}
                        self.checksum = 0
                        return frame
        return None

    def _verify_checksum(self) -> bool:
        """Verify VE.Direct frame checksum (sum of all bytes mod 256 == 0)."""
        try:
            return self.checksum & 0xFF == 0
        except Exception:
            return False


class VeDirectShuntMonitor:
    def __init__(self, shunt_port):
        self.port = shunt_port
        self.ser = None
        self.parser = VeDirectParser()
        self.data: VeDirectShuntData = None

    def __del__(self):
        if self.ser is not None:
            self.ser.close()

    def update(self) -> Optional[VeDirectShuntData]:
        if self.ser is not None:
            if self._check_for_interference():
                # On interference, reopen the port to reset the port settings.
                logging.warning("Interference detected, reopening serial port")
                self.ser.close()
                self.ser = None

        if self.ser is None:
            try:
                self.ser = serial.Serial(
                    port=self.port,
                    baudrate=SHUNT_BAUD_RATE,
                    bytesize=serial.EIGHTBITS,
                    parity=serial.PARITY_NONE,
                    stopbits=serial.STOPBITS_ONE,
                    timeout=SERIAL_TIMEOUT_SECONDS,
                )
            except Exception:
                logging.exception("Couldn't open serial port")
                return
        try:
            data = self.ser.read(self.ser.in_waiting or 1)
            if data:
                self.parser.feed(data)
        except Exception:
            logging.exception("Couldn't read shunt data from serial port")

        while (frame := self.parser.next_frame()) is not None:
            if not frame[VeKey.CHECKSUM_VALID.value]:
                continue

            consumed_ah = self._parse_int(frame, VeKey.CONSUMED_MAH, 1000.0)
            current_amps = self._parse_int(frame, VeKey.CURRENT_MA, 1000.0)
            soc_percent = self._parse_int(frame, VeKey.SOC_PERMILLE, 10.0)

            if consumed_ah is not None and current_amps is not None and soc_percent is not None:
                self.data = VeDirectShuntData(consumed_ah=consumed_ah, current_amps=current_amps, soc_percent=soc_percent, read_timestamp=time.monotonic())

        # If there's no update or update failed this time, return previous data, but only if it's recent enough.
        return None if self.data is None or self.data.read_timestamp < time.monotonic() - DATA_EXPIRATION_SECONDS else self.data

    @staticmethod
    def _parse_int(frame, key_enum: VeKey, divisor: float) -> Optional[float]:
        value = frame.get(key_enum.value)
        if value:
            try:
                return int(value) / divisor
            except ValueError:
                logging.exception(f"Couldn't parse {key_enum.name} from the shunt: {value}")
        return None

    def _check_for_interference(self) -> bool:
        """Returns True if another process changed the serial port settings."""
        try:
            attr = termios.tcgetattr(self.ser)
            return attr[4] != termios.B19200 or attr[5] != termios.B19200 or attr[2] & termios.CSIZE != termios.CS8
        except Exception:
            logging.exception("Couldn't check whether there's serial connection interference")
            return False
