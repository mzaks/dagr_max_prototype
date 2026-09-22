# A Dagr SinkDestination that writes into a shared memory mapping of the log file.
#
# Records are copied into a MAP_SHARED mapping of a pre-sized file (grown in `chunk` steps),
# so no write system call happens per record or per flush: bytes are in the OS page cache as
# soon as `write` returns, visible to readers of the file and kept if the process dies.
#
# A pre-sized file has a zero-filled tail, so readers cannot use the file size. The committed
# length (bytes of whole records) lives in an 8-byte sidecar file `<path>.len`, itself mapped
# and updated AFTER the record bytes are in place. Readers read the sidecar, then read the log
# up to that length. `close()` trims the log to the committed length, which makes it a plain
# Dagr stream again.
#
# The mapping itself is mm_mmap (third_party/mm_mmap, Apache-2.0), which carries the per-OS
# constants; what stays here is the file plumbing — create, size, trim — and the growth policy.
from std.ffi import c_int, external_call
from std.io.file import FileHandle
from std.memory import unsafe_memcpy
from std.sys.info import platform_map

from dagr_writer import SinkDestination
from mm_mmap import MAP_SHARED, MemoryMap, PROT_READ, PROT_WRITE

comptime _O_RDWR: Int32 = 2
# openat(2) relative to the working directory; the value differs per OS.
comptime _AT_FDCWD = Int32(platform_map[T=Int, "AT_FDCWD", linux=-100, macos=-2]())


def _open_rdwr(path: String) raises -> Int32:
    # Create/truncate through FileHandle, then reopen O_RDWR without O_CREAT so the variadic
    # `mode` argument of open(2) is never needed. `openat` rather than `open`: the Mojo stdlib
    # already declares `open` with another signature.
    var f = open(path, "w")
    f.close()
    var p = path.copy()
    var fd = external_call["openat", Int32](_AT_FDCWD, p.as_c_string_span().ptr(), _O_RDWR)
    if fd < 0:
        raise Error("open failed: ", path)
    return fd


def _truncate(fd: Int32, size: Int) raises:
    if external_call["ftruncate", Int32](fd, Int64(size)) != 0:
        raise Error("ftruncate failed")


def _map(fd: Int32, size: Int) raises -> MemoryMap:
    _truncate(fd, size)
    return MemoryMap.map_fd(Int(fd), size, prot=PROT_READ | PROT_WRITE, flags=MAP_SHARED)


struct MmapFileDestination(SinkDestination):
    var fd: Int32
    var map: MemoryMap
    var size: Int
    var committed: Int
    var chunk: Int
    var len_fd: Int32
    var len_map: MemoryMap
    var closed: Bool
    var msync: Bool
    """Whether flush() msyncs. Off: a flush is a no-op and the page cache is the durability
    boundary (a process crash loses nothing, an OS crash may)."""

    def __init__(out self, path: String, chunk: Int = 64 << 20, msync: Bool = True) raises:
        self.chunk = chunk
        self.msync = msync
        self.fd = _open_rdwr(path)
        self.size = chunk
        self.map = _map(self.fd, chunk)
        self.committed = 0
        self.len_fd = _open_rdwr(path + ".len")
        self.len_map = _map(self.len_fd, 8)
        self.closed = False
        self._publish_length()

    @always_inline
    def _publish_length(mut self):
        self.len_map.unsafe_ptr().unsafe_bitcast[UInt64]()[] = UInt64(self.committed)

    def _grow(mut self, need: Int) raises:
        var new_size = self.size
        while new_size < need:
            new_size += self.chunk
        self.map = _map(self.fd, new_size)   # the previous mapping unmaps on replacement
        self.size = new_size

    def write(mut self, bytes: Span[UInt8, _]) raises:
        var n = len(bytes)
        var end = self.committed + n
        if end > self.size:
            self._grow(end)
        unsafe_memcpy(
            dest=self.map.unsafe_ptr().unsafe_offset(self.committed),
            src=bytes.unsafe_ptr(),
            count=n,
        )
        self.committed = end
        self._publish_length()

    def flush(mut self) raises:
        """msync the log, then the sidecar.

        Without this the records are only in the page cache: durable against a process crash,
        not against an OS crash. The order matters — the committed length must not reach the
        disk before the bytes it points at. Cost tracks dirty pages, not mapping size: ~34 us
        blocking for 100 KB dirty, whether the mapping is 1 MB or 64 MB.
        """
        if not self.msync:
            return
        self.map.flush()
        self.len_map.flush()

    def close(mut self) raises:
        if self.closed:
            return
        self.closed = True
        self.flush()
        _truncate(self.fd, self.committed)
        _ = external_call["close", c_int](self.fd)
        _ = external_call["close", c_int](self.len_fd)
