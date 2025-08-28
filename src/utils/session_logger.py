"""Atomic session logger - no state tracking."""

from datetime import datetime
from pathlib import Path
from typing import List, Optional


def log_session(focus: str, tasks: List[str], notes: Optional[str] = None) -> None:
    """Log a complete work session atomically."""
    log = Path("PROJECT_LOG.md")
    log.touch(exist_ok=True)
    
    with open(log, "a") as f:
        f.write(f"\n---\n\n")
        f.write(f"## Session: {datetime.now():%Y-%m-%d %H:%M}\n")
        f.write(f"**Focus**: {focus}\n\n")
        
        if tasks:
            f.write("### Completed\n")
            for task in tasks:
                f.write(f"- {task}\n")
            f.write("\n")
        
        if notes:
            f.write(f"**Notes**: {notes}\n")