import subprocess
import sys


def test_llm_wiki_is_structurally_consistent():
    result = subprocess.run(
        [sys.executable, "scripts/lint_wiki.py"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
