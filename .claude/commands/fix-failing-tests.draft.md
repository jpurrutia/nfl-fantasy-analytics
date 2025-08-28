# DRAFT - NOT READY FOR USE
# This command is under development and should not be used yet
# Rename to fix-failing-tests.md when implementation is complete

# Fix Failing Tests

Debug and fix failing tests: $ARGUMENTS

Follow this systematic approach to resolve test failures:

## 1. Run All Tests
```bash
uv run pytest tests/ -xvs
```
- Note which tests are failing
- Capture error messages
- Identify patterns in failures

## 2. Run Specific Test File
```bash
# If specific test file is provided in $ARGUMENTS
uv run pytest tests/[test_file.py] -xvs
```

## 3. Debug Individual Test
```bash
# Run single test with maximum verbosity
uv run pytest tests/[test_file.py]::[TestClass]::[test_method] -xvs --tb=long
```

## 4. Common Fixes

### Database Connection Issues
- Check if test database exists
- Verify fixtures are setting up properly
- Ensure cleanup between tests

### Import Errors
- Verify module paths are correct
- Check for missing dependencies
- Ensure `__init__.py` files exist

### Assertion Failures
- Review expected vs actual values
- Check test data setup
- Verify business logic changes

### Fixture Problems
- Check fixture scope (function/class/module/session)
- Verify fixture dependencies
- Ensure proper teardown

## 5. Test Coverage Check
```bash
uv run pytest tests/ --cov=src --cov-report=term-missing
```

## 6. Update Tests
- Fix deprecated assertions
- Update mocked responses
- Adjust for schema changes

## 7. Verify Fix
```bash
# Run full test suite again
uv run pytest tests/ -xvs

# Run with different Python warnings
uv run pytest tests/ -W error
```

## 8. Document Changes
- Update docstrings if behavior changed
- Add comments for complex fixes
- Update PROJECT_LOG.md with resolution

## Future Implementation Needed:
- [ ] Add common test failure patterns specific to this project
- [ ] Integrate with CI/CD failure reports
- [ ] Add automated fix suggestions
- [ ] Create test failure categorization