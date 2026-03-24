from pathlib import Path


def test_layout__dags_directory__exists_at_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    dags_path = repo_root / "dags"

    assert dags_path.is_dir()
