# coding: utf-8

# Third Party Libraries
import pbr.version

__version__ = pbr.version.VersionInfo("git-cdn").release_string()
VERSION = __version__

__all__ = [
    "__version__",
    "VERSION",
]
