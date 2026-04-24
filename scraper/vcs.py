import os
import subprocess
from pathlib import Path

import structlog

log = structlog.get_logger()


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=str(cwd), capture_output=True, text=True)


def ensure_git_identity(repo_dir: Path) -> None:
    user_name = os.environ.get("GIT_USER_NAME", "community-scraper-bot")
    user_email = os.environ.get("GIT_USER_EMAIL", "bot@localhost")
    _run(["git", "config", "--local", "user.name", user_name], repo_dir)
    _run(["git", "config", "--local", "user.email", user_email], repo_dir)


def ensure_repo(repo_dir: Path) -> None:
    git_dir = repo_dir / ".git"
    if not git_dir.exists():
        log.info("initializing_git_repo", path=str(repo_dir))
        _run(["git", "init"], repo_dir)
        remote = os.environ.get("GIT_REMOTE_URL", "")
        if remote:
            token = os.environ.get("GIT_TOKEN", "")
            if token and "github.com" in remote:
                parts = remote.split("://", 1)
                remote = f"{parts[0]}://{token}@{parts[1]}"
            _run(["git", "remote", "add", "origin", remote], repo_dir)
    ensure_git_identity(repo_dir)


def has_changes(repo_dir: Path, path: str = "data/") -> bool:
    result = _run(["git", "status", "--porcelain", path], repo_dir)
    return bool(result.stdout.strip())


def commit_data(repo_dir: Path, message: str) -> bool:
    if not has_changes(repo_dir):
        log.info("no_changes_to_commit")
        return False

    _run(["git", "add", "data/"], repo_dir)
    result = _run(["git", "commit", "-m", message], repo_dir)
    if result.returncode != 0:
        log.error("git_commit_failed", stderr=result.stderr)
        return False

    log.info("git_committed", message=message)

    if os.environ.get("PUSH_AFTER_COMMIT", "false").lower() == "true":
        _push(repo_dir)

    return True


def _push(repo_dir: Path) -> None:
    token = os.environ.get("GIT_TOKEN", "")
    remote = os.environ.get("GIT_REMOTE_URL", "")
    if token and remote:
        parts = remote.split("://", 1)
        auth_remote = f"{parts[0]}://{token}@{parts[1]}"
        _run(["git", "remote", "set-url", "origin", auth_remote], repo_dir)

    result = _run(["git", "push", "origin", "main"], repo_dir)
    if result.returncode != 0:
        log.error("git_push_failed", stderr=result.stderr)
    else:
        log.info("git_pushed")
