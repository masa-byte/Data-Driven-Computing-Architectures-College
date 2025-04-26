# Items in the test directory
- *logs* subdirectory

## logs subdirectory
Logs represents our logging mechanism, and code for its creation is in [logger.py](../code/logger.py).
Every notebook from the *code* directory has its own log file in the *logs* directory.
The logs are used to store the output of the notebooks.
They document the following structure:
- The name of the notebook (determines which file the log message belongs to)
- Level of log (INFO, WARNING, ERROR)
- The calling function
- The message
- The time of the log message
