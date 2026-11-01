"""Make the DAG folder importable (the DAGs import sibling modules such as sheets_export)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
