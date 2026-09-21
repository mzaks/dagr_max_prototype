"""macOS thread QoS helpers (libSystem): read and set the calling thread's QoS class."""
import ctypes

NAMES = {0x21: "USER_INTERACTIVE", 0x19: "USER_INITIATED", 0x15: "DEFAULT", 0x11: "UTILITY",
         0x09: "BACKGROUND", 0x00: "UNSPECIFIED"}
CLASSES = {v: k for k, v in NAMES.items()}
_lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
_lib.qos_class_self.restype = ctypes.c_uint
_lib.pthread_set_qos_class_self_np.argtypes = [ctypes.c_uint, ctypes.c_int]


def current() -> str:
    q = _lib.qos_class_self()
    return NAMES.get(q, hex(q))


def set_self(name: str) -> int:
    return _lib.pthread_set_qos_class_self_np(CLASSES[name], 0)
