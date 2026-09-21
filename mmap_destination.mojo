# A Dagr SinkDestination that writes into a shared memory mapping of the log file.
#
# Records are copied into a MAP_SHARED mapping of a pre-sized file (grown in `chunk` steps),
# so no write system call happens per record or per flush: bytes are in the OS page cache as
# soon as `write` returns, visible to readers of the file and kept if the process dies.
#
# A pre-sized file has a zero-filled tail, so readers cannot use the file size. The committed
# length (bytes of whole records) lives in an 8-byte sidecar file `<path>.len`, itself mapped
# and updated with one store AFTER the record bytes are in place. Readers read the sidecar,
# then read the log up to that length. `close()` trims the log to the committed length, which
# makes it a plain Dagr stream again.
from std.ffi import external_call
from std.io.file import FileHandle
from std.memory import unsafe_memcpy

from dagr_writer import SinkDestination

comptime _O_RDWR: Int32 = 2
comptime _AT_FDCWD: Int32 = -2      # macOS; Linux is -100
comptime _PROT_RW: Int32 = 3        # PROT_READ | PROT_WRITE
comptime _MAP_SHARED: Int32 = 1


def _open_rdwr(path: String) raises -> Int32:
    # Create/truncate through FileHandle, then reopen O_RDWR without O_CREAT so the
    # variadic `mode` argument of open(2) is never needed. macOS only (AT_FDCWD).
    var f = open(path, "w")
    f.close()
    var p = path.copy()
    # openat: the stdlib already declares `open` with another signature.
    var fd = external_call["openat", Int32](_AT_FDCWD, p.as_c_string_slice().ptr(), _O_RDWR)
    if fd < 0:
        raise Error("open failed: ", path)
    return fd


def _map(fd: Int32, size: Int) raises -> Int:
    if external_call["ftruncate", Int32](fd, Int64(size)) != 0:
        raise Error("ftruncate failed")
    var addr = external_call["mmap", Int](Int(0), size, _PROT_RW, _MAP_SHARED, fd, Int64(0))
    if addr == -1:
        raise Error("mmap failed")
    return addr


struct MmapFileDestination(SinkDestination):
    var fd: Int32
    var addr: Int
    var size: Int
    var committed: Int
    var chunk: Int
    var len_fd: Int32
    var len_addr: Int
    var closed: Bool

    def __init__(out self, path: String, chunk: Int = 64 << 20) raises:
        self.chunk = chunk
        self.fd = _open_rdwr(path)
        self.size = chunk
        self.addr = _map(self.fd, chunk)
        self.committed = 0
        self.len_fd = _open_rdwr(path + ".len")
        self.len_addr = _map(self.len_fd, 8)
        self.closed = False
        self._publish_length()

    @always_inline
    def _publish_length(self):
        Pointer[UInt64, MutAnyOrigin](unsafe_from_address=self.len_addr)[] = UInt64(self.committed)

    def _grow(mut self, need: Int) raises:
        var new_size = self.size
        while new_size < need:
            new_size += self.chunk
        _ = external_call["munmap", Int32](self.addr, self.size)
        self.addr = _map(self.fd, new_size)
        self.size = new_size

    def write(mut self, bytes: Span[UInt8, _]) raises:
        var n = len(bytes)
        var end = self.committed + n
        if end > self.size:
            self._grow(end)
        unsafe_memcpy(
            dest=Pointer[UInt8, MutAnyOrigin](unsafe_from_address=self.addr + self.committed),
            src=bytes.unsafe_ptr(),
            count=n,
        )
        self.committed = end
        self._publish_length()

    def flush(mut self) raises:
        pass   # bytes are already in the page cache; durability against OS crash would be msync

    def close(mut self) raises:
        if self.closed:
            return
        self.closed = True
        _ = external_call["munmap", Int32](self.addr, self.size)
        _ = external_call["ftruncate", Int32](self.fd, Int64(self.committed))
        _ = external_call["close", Int32](self.fd)
        _ = external_call["munmap", Int32](self.len_addr, 8)
        _ = external_call["close", Int32](self.len_fd)
