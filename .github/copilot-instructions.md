# Copilot Instructions for xGT Shell (xgtsh)

This document provides guidance for GitHub Copilot when working on the xGT Shell project.

## Project Overview

This is a command-line interface (CLI) shell for interacting with Trovares xGT graph database servers. The main file is `src/xgtsh.py` which implements a `cmd.Cmd`-based interactive shell.

## Key Project Context

### Technology Stack
- **Language**: Python 3
- **Main Framework**: `cmd.Cmd` for the command-line interface
- **External Dependencies**:
  - `xgt` - Trovares xGT Python client library
  - `pandas` (optional) - For data display formatting
  - `readline`/`pyreadline` (optional) - For command history and completion

### Architecture
- Single main class `XgtCli` that extends `cmd.Cmd`
- Each command is implemented as a `do_<command>` method
- Tab completion methods follow the pattern `complete_<command>`
- Connection management through `xgt.Connection` objects

## Important Coding Guidelines

### xGT API Usage
- **ALWAYS use the latest xGT API methods**:
  - Use `get_frame()` instead of `get_edge_frame()`, `get_vertex_frame()`, or `get_table_frame()`
  - Use `get_frames(namespace=ns, frame_type='edge/vertex/table')` instead of `get_edge_frames()`, `get_vertex_frames()`, or `get_table_frames()`
  - Use `job.get_data()` followed by `pd.DataFrame(data)` instead of deprecated `job.get_data_pandas()`
  - Use `drop_frames()` for bulk operations when server version supports it

### Error Handling
- Always check if `self.__server` is None before making server calls
- Use try/except blocks when accessing frames that might not exist
- Provide helpful error messages with usage examples

### Command Method Patterns
- All `do_<command>` methods should return `False` (don't exit shell)
- Parse command line arguments carefully with proper validation
- Include docstrings that serve as help text
- Handle both verbose and non-verbose output modes

### Code Style
- Use f-strings for string formatting
- Follow existing indentation (2 spaces)
- Keep method signatures consistent with type hints where used
- Use descriptive variable names

## Common Development Tasks

### Adding New Commands
1. Create a `do_<command>` method with proper docstring
2. Parse arguments from the `line` parameter
3. Add server connection checks
4. Implement the command logic
5. Consider adding tab completion with `complete_<command>` method

### Updating API Calls
- Check Trovares documentation at docs.trovares.com for latest API
- Look for version checks like `__version_is_since()` for backward compatibility
- Test with both pandas available and unavailable scenarios

### Error Handling Best Practices
- Always provide usage examples in error messages
- Use the `self.prompt` variable in usage messages for consistency
- Handle both connection errors and xGT-specific errors

## Testing Considerations
- Test with and without pandas installed (`HASPANDAS` flag)
- Test with and without readline available (`READLINE_DEFINED` flag)
- Verify commands work with different server versions
- Test tab completion functionality

## Version Compatibility
- The code includes version checking logic for backward compatibility
- When adding new features, consider using `__version_is_since()` checks
- Maintain support for older xGT server versions where reasonable

## Command Line Usage

The xGT Shell can be used both interactively and non-interactively:

### Interactive Mode
```bash
./src/xgtsh                    # Start interactive shell
./src/xgtsh -v                 # Start with verbose output
./src/xgtsh --host remote      # Connect to remote server
```

### Non-Interactive Modes
```bash
# Execute a single Cypher query
./src/xgtsh -q "MATCH (n) RETURN count(n)"

# Execute with different output formats
./src/xgtsh -q "MATCH (n) RETURN n LIMIT 5" --format json
./src/xgtsh -q "MATCH (n) RETURN n LIMIT 5" --format csv

# Execute a single xGT shell command
./src/xgtsh -c "show default"
./src/xgtsh -c "memory"

# Execute commands from a file
./src/xgtsh -f script.xgt
```

### Output Formats
- `table` (default) - Formatted table using pandas if available
- `json` - JSON formatted output
- `csv` - Comma-separated values

## Common Patterns to Follow

### Server Connection Check
```python
if self.__server is None:
  print("Not connected to a server")
  return False
```

### Frame Existence Check
```python
try:
  frame = self.__server.get_frame(frame_name)
except:
  print(f"Frame {frame_name} does not exist")
  return False
```

### Pandas Data Display
```python
if HASPANDAS:
  data = job.get_data()
  df = pd.DataFrame(data)
  print(df)
else:
  data = job.get_data()
  pprint.pprint(data)
```

## Documentation References
- Trovares xGT API Documentation: docs.trovares.com
- Python cmd module: https://docs.python.org/3/library/cmd.html

## Notes for Future Development
- The project uses optional dependencies (pandas, readline) - always check availability
- Command history is automatically saved to `~/.xgthist`
- The shell supports namespace-aware tab completion
- Consider adding more sophisticated data visualization features
- Authentication uses xGT BasicAuth - may need updates for new auth methods
