# Vendored dependencies

## mm_mmap

POSIX memory mapping for Mojo — https://github.com/Mojo-Mania/mm_mmap (Apache-2.0),
vendored at upstream commit 2806959.

Upstream installs as a pixi git dependency. It is vendored here because this project builds
through plain `mojo build -I …` rather than a pixi environment.
