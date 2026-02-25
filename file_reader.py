import os
import re
import sys

# Matches a trailing inline comment: one-or-more whitespace chars + # + rest.
# The leading whitespace requirement prevents stripping # inside unquoted values
# that immediately follow non-whitespace (e.g. a literal "#" right after a char).
_INLINE_COMMENT_RE = re.compile(r'\s+#.*$')


def _strip_inline_comment(line: str) -> str:
    """Remove a trailing inline comment from *line*.

    Only strips ``# ...`` that are preceded by at least one whitespace
    character, so ``replace col "#" "x"`` is left untouched while
    ``source "file.csv"  # load data`` is cleaned to ``source "file.csv"``.
    """
    return _INLINE_COMMENT_RE.sub("", line)


def read_ppl_file(file_path: str) -> list[str]:
    """Read a .ppl pipeline file and return cleaned lines.

    For each raw line the function:

    1. Strips surrounding whitespace.
    2. Discards blank lines and full-line comment lines (starting with ``#``).
    3. Strips trailing inline comments (whitespace + ``#`` + rest).

    Args:
        file_path: Path to the ``.ppl`` file.

    Returns:
        A list of non-empty, non-comment lines ready for parsing.

    Raises:
        ValueError: If *file_path* does not end with ``.ppl``.
        FileNotFoundError: If *file_path* does not exist on disk.
        IOError: If the file cannot be read for any other reason.
    """
    if not file_path.endswith(".ppl"):
        raise ValueError(f"Expected a .ppl file, got: '{file_path}'")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Pipeline file not found: '{file_path}'")

    with open(file_path, "r", encoding="utf-8") as fh:
        raw_lines = fh.readlines()

    cleaned = []
    for raw in raw_lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = _strip_inline_comment(line)
        if line:
            cleaned.append(line)
    return cleaned


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python file_reader.py <file_path>")
        sys.exit(1)

    try:
        lines = read_ppl_file(sys.argv[1])
        print(f"Successfully read {len(lines)} line(s):")
        for ln in lines:
            print(f"  {ln}")
    except (ValueError, FileNotFoundError, IOError) as exc:
        print(f"Error: {exc}")
        sys.exit(1)
