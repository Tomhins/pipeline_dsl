"""Entry point for the pipeline DSL runner.

Usage
-----
.. code-block:: console

    python main.py pipelines/example.ppl

The script:

1. Reads and cleans the ``.ppl`` file via :func:`~file_reader.read_ppl_file`.
2. Converts the lines to AST nodes via :func:`~parser.parse_lines`.
3. Executes the pipeline via :func:`~executor.run_pipeline`.
4. Prints a success summary and a preview of the output.
"""

from __future__ import annotations

import os
import sys

from executor import run_pipeline
from file_reader import read_ppl_file
from ppl_parser import parse_lines


def main(argv: list[str] | None = None) -> int:
    """Run a ``.ppl`` pipeline file from the command line.

    Args:
        argv: Argument list (defaults to :data:`sys.argv` when ``None``).

    Returns:
        Exit code: ``0`` on success, ``1`` on any error.
    """
    args = argv if argv is not None else sys.argv[1:]

    if not args:
        print("Usage: python main.py <pipeline_file.ppl>")
        print("Example: python main.py pipelines/example.ppl")
        return 1

    ppl_file = args[0]

    # ------------------------------------------------------------------ #
    # Step 1 – read the .ppl file                                         #
    # ------------------------------------------------------------------ #
    try:
        lines = read_ppl_file(ppl_file)
    except (ValueError, FileNotFoundError, IOError) as exc:
        print(f"Error reading pipeline file: {exc}")
        return 1

    if not lines:
        print(f"Warning: '{ppl_file}' contains no executable commands.")
        return 0

    # Change working directory to the folder containing the .ppl file so
    # that relative paths inside it (e.g. source "data/people.csv") resolve
    # correctly regardless of where the ppl command was invoked from.
    ppl_dir = os.path.dirname(os.path.abspath(ppl_file))
    os.chdir(ppl_dir)

    print(f"Loaded {len(lines)} command(s) from '{ppl_file}'.")

    # ------------------------------------------------------------------ #
    # Step 2 – parse lines into AST nodes                                 #
    # ------------------------------------------------------------------ #
    try:
        nodes = parse_lines(lines)
    except SyntaxError as exc:
        print(f"Parse error: {exc}")
        return 1

    print(f"Parsed {len(nodes)} AST node(s).")

    # ------------------------------------------------------------------ #
    # Step 3 – execute the pipeline                                       #
    # ------------------------------------------------------------------ #
    try:
        result_df = run_pipeline(nodes)
    except Exception as exc:  # noqa: BLE001
        print(f"Execution error: {exc}")
        return 1

    # ------------------------------------------------------------------ #
    # Step 4 – report results                                             #
    # ------------------------------------------------------------------ #
    print("\nPipeline completed successfully.")

    if result_df is not None and not result_df.empty:
        rows, cols = result_df.shape
        print(f"Output: {rows} row(s) × {cols} column(s).")
        print("\nPreview (first 10 rows):")
        print(result_df.head(10).to_string(index=False))
    elif result_df is not None and result_df.empty:
        print("Output is an empty DataFrame (all rows were filtered out).")
    else:
        print("Pipeline produced no output.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
