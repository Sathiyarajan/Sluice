"""spark-submit requires a script path, not `python -m`; this just forwards to
the real CLI entrypoint (ingestion.run_pipeline.main) unchanged."""
import sys

from ingestion.run_pipeline import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
