# Vendored dependencies

## mm_mmap

POSIX memory mapping for Mojo — https://github.com/Mojo-Mania/mm_mmap (Apache-2.0),
vendored at upstream commit 2806959.

Upstream installs as a pixi git dependency. It is vendored here because this project builds
with two toolchains (MAX's Mojo for the in-process extension, flare's for the standalone
endpoint) through plain `mojo build -I …` rather than a pixi environment. Both compile it
unchanged.
