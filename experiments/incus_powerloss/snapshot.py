"""Stream the current working tree, including nonignored untracked files, as tar."""

import os
from pathlib import Path
import subprocess
import sys
import tarfile


def main():
    root = Path(sys.argv[1])
    names = subprocess.check_output(
        [
            "git",
            "-C",
            str(root),
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ]
    ).split(b"\0")
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
        for name in dict.fromkeys(os.fsdecode(name) for name in names if name):
            path = root / name
            if path.exists() or path.is_symlink():
                archive.add(path, arcname=name, recursive=False)


if __name__ == "__main__":
    main()
