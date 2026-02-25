import os
import sys


def read_ppl_file(file_path: str) -> list[str]:
    """Read a .ppl pipeline file and return cleaned lines.

    Strips whitespace from each line and discards blank lines and
    comment lines (those beginning with ``#``).

    Args:
        file_path: Path to the ``.ppl`` file.

    Returns:
        A list of non-empty, non-comment lines with surrounding
        whitespace removed.

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

    return [
        line.strip()
        for line in raw_lines
        if line.strip() and not line.strip().startswith("#")
    ]


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
