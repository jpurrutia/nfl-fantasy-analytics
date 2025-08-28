# End Work Session

Complete current work session with summary: $ARGUMENTS

Follow these steps:

1. **Review work completed**:
   - Check TodoWrite list for completion status
   - Document any incomplete tasks for next session
   
2. **Update PROJECT_LOG.md**:
   - Mark current session as "Complete"
   - List all completed tasks
   - Calculate session duration
   - Add summary from $ARGUMENTS
   - Document any unresolved issues
   
3. **Run quality checks**:
   - Execute `ruff check . --fix` for linting
   - Execute `ruff format .` for formatting
   - Run tests with `uv run pytest tests/ -xvs`
   
4. **Log checks**:
   - Review PROJECT_LOG.md for completeness
   - Ensure all major decisions are documented
   - Verify session timestamps are accurate
   
5. **Update documentation and next steps**:
   - Update relevant docs (README.md, CLAUDE.md) if needed
   - Add "Next Steps" section to PROJECT_LOG.md
   - Note any blockers or dependencies
   - Set focus for next session
   
6. **Commit outstanding changes** (if ready):
   - First run `git status` to review all changes
   - Review diffs with `git diff` for uncommitted changes
   - If changes look good, create meaningful commit message
   - Include session reference in commit message
   - Only commit if you're satisfied with the changes
   
7. **Final verification**:
   - Ensure all tests pass
   - Verify database integrity if data was modified
   - Check that documentation is updated