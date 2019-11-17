import datetime
import os
import struct
from pathlib import Path

import mmap

DEFAULT_MAX_JOURNAL_SIZE = 64 * 1024 * 1024  # 64MB


class NoSpaceException(Exception):
    pass


class Journal:
    def __init__(self, base_path: Path, max_size: int = DEFAULT_MAX_JOURNAL_SIZE):
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.max_size = max_size

    def create_reader(self, date: datetime.date = datetime.datetime.utcnow().date()):
        return JournalReader(self, self._get_mmap(date, 'r+b'))

    def create_appender(self):
        return JournalAppender(self)

    def _get_mmap(self, date: datetime.date, mode: str):
        mmap_path = self._get_mmap_path(date)
        if mode == 'a+b' and not mmap_path.exists():
            # switch to file create mode if path does not yet exist
            mmap_path.parent.mkdir(parents=True, exist_ok=True)
            mode = 'w+b'

        mmap_file = mmap_path.open(mode=mode)
        if mode == 'w+b':
            # zero-fill the entire memory-map file before memory-mapping it
            os.write(mmap_file.fileno(), struct.pack('B', 0) * self.max_size)
            mmap_file.flush()
        return self._mmap(mmap_file)

    def _mmap(self, mmap_file):
        return mmap.mmap(mmap_file.fileno(), self.max_size)

    def _get_mmap_path(self, date: datetime.date):
        return self.base_path.joinpath(Path('%4d%02d%02d/journal.dat' % (date.year, date.month, date.day)))


class JournalReader:
    def __init__(self, journal: Journal, mm, offset: int = 4):
        self.journal = journal
        self.offset = offset
        self.mm = mm

    def get_length(self):
        return ~struct.unpack('i', self.mm[0:4])[0]

    def get_offset(self):
        return self.offset

    def read_byte(self) -> int:
        ret = self.mm[self.offset]
        self._advance(1)
        return ret

    def read_boolean(self) -> bool:
        return self.read_byte() != 0

    def read_short(self) -> int:
        val_sz = 2
        ret = struct.unpack('h', self.mm[self.offset:self.offset + val_sz])[0]
        self._advance(val_sz)
        return ret

    def read_int(self) -> int:
        val_sz = 4
        ret = struct.unpack('i', self.mm[self.offset:self.offset + val_sz])[0]
        self._advance(val_sz)
        return ret

    def read_long(self) -> int:
        val_sz = 8
        ret = struct.unpack('q', self.mm[self.offset:self.offset + val_sz])[0]
        self._advance(val_sz)
        return ret

    def read_float(self) -> float:
        val_sz = 4
        ret = struct.unpack('f', self.mm[self.offset:self.offset + val_sz])[0]
        self._advance(val_sz)
        return ret

    def read_double(self) -> float:
        val_sz = 8
        ret = struct.unpack('d', self.mm[self.offset:self.offset + val_sz])[0]
        self._advance(val_sz)
        return ret

    def read_string(self) -> str:
        val_sz = self._read_stopbit()
        ret = self.mm[self.offset: self.offset + val_sz].decode()
        self._advance(val_sz)
        return ret

    def _read_stopbit(self) -> int:
        shift = 0
        value = 0
        while True:
            b = self.read_byte()
            value += (b & 0x7f) << shift
            shift += 7
            if (b & 0x80) == 0:
                return value

    def _advance(self, step):
        self.offset += step


class JournalAppender:
    def __init__(self, journal: Journal):
        self.journal = journal
        self.pos = 4
        self.start_post = self.pos
        self.max_size = journal.max_size

        self.current_date = None
        self.mm = None

    def write_byte(self, value: int):
        assert value < 256
        val_sz = 1
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos] = value
        self._advance(val_sz)

    def write_boolean(self, value: bool):
        val_sz = 1
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos] = 1 if value else 0
        self._advance(val_sz)

    def write_short(self, value: int):
        val_sz = 2
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = struct.pack('h', value)
        self._advance(val_sz)

    def write_int(self, value: int):
        val_sz = 4
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = struct.pack('i', value)
        self._advance(val_sz)

    def write_long(self, value: int):
        val_sz = 8
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = struct.pack('q', value)
        self._advance(val_sz)

    def write_float(self, value: float):
        val_sz = 4
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = struct.pack('f', value)
        self._advance(val_sz)

    def write_double(self, value: float):
        val_sz = 8
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = struct.pack('d', value)
        self._advance(val_sz)

    def write_string(self, value: str):
        encoded = value.encode()
        val_sz = len(encoded)
        self._write_stopbit(val_sz)
        self._check_space(val_sz)
        self._get_current_mmap()[self.pos:self.pos + val_sz] = encoded
        self._advance(val_sz)

    def close(self):
        if self.mm:
            length = self.pos - self.start_pos
            self.mm[0:4] = struct.pack('i', ~length)
            self.mm.close()
            self.mm = None

    def _write_stopbit(self, value):
        if value < 0:
            raise ValueError('Stop-bit encoding does not support negative values')
        mm = self._get_current_mmap()
        while value > 127:
            mm[self.pos] = 0x80 | (value & 0x7f)
            self._advance(1)
            value >>= 7
        mm[self.pos] = value
        self._advance(1)

    # noinspection PyProtectedMember
    def _get_current_mmap(self):
        now_date = datetime.datetime.utcnow().date()
        if self.current_date != now_date:
            # finish writing the file and unmap
            self.close()

            # initialize the new mmap
            self.current_date = now_date
            self.pos = 4
            self.start_pos = self.pos
            self.mm = self.journal._get_mmap(self.current_date, mode='a+b')

        return self.mm

    def _advance(self, num_bytes: int):
        self.pos += num_bytes

    def _check_space(self, add_length: int):
        if self.pos + add_length >= self.max_size:
            raise NoSpaceException()
