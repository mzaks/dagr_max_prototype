# ===----------------------------------------------------------------------=== #
# Copyright (c) 2026, Maxim Zaks. All rights reserved.
#
# Licensed under the Apache License, Version 2.0:
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===----------------------------------------------------------------------=== #
"""POSIX memory mapping for Mojo.

Maps a file or fresh anonymous memory into the process address space with an
owning, RAII `MemoryMap` type:

```mojo
from mm_mmap import MemoryMap, PROT_READ, MAP_PRIVATE

with open("data.bin", "r") as f:
    var m = MemoryMap.map(f, prot=PROT_READ, flags=MAP_PRIVATE)
    print(m.bytes()[0])
```
"""

from .mmap import (
    MemoryMap,
    Prot,
    MapFlags,
    PROT_NONE,
    PROT_READ,
    PROT_WRITE,
    PROT_EXEC,
    MAP_SHARED,
    MAP_PRIVATE,
    MAP_FIXED,
    MAP_ANONYMOUS,
    page_size,
)
