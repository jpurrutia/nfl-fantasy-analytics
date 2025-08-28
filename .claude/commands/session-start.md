# Start Work Session

Start a new work session for: $ARGUMENTS

Follow these steps:

1. **Update PROJECT_LOG.md** with new session entry:
   - Add timestamp with `date +"%Y-%m-%d %H:%M"`
   - Set status to "In Progress"
   - Define focus area from $ARGUMENTS
   
2. **Check git status** to understand current state:
   - Run `git status` to see uncommitted changes
   - Run `git log --oneline -5` to see recent commits
   
3. **Review previous session** if applicable:
   - Check PROJECT_LOG.md for incomplete tasks
   - Identify any unresolved blockers
   - Note what needs continuation
   
4. **Create todo list** for session objectives:
   - Use TodoWrite tool to track tasks
   - Include any incomplete items from previous session
   - Break down complex work into subtasks
   
5. **Set up environment**:
   - Run `uv run python -m src.cli.main status` to check database
   - Ensure all dependencies are installed

Remember to follow the explore-plan-code-commit workflow for all tasks.