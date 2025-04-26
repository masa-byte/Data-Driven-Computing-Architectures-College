import os
import datetime

# Create logs directory if it doesn't exist
log_dir = "/Workspace/Users/masa.cirkovic@abo.fi/logs"
os.makedirs(log_dir, exist_ok=True)


# Function to log messages dynamically
def log_message(level, notebook, function, message):
    log_file_path = os.path.join(
        log_dir, f"{notebook}.log"
    )  # Separate file per notebook
    log = ""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if level == "info":
        log = f"INFO: {function} | {message} | {timestamp}"
    elif level == "warning":
        log = f"WARNING: {function} | {message} | {timestamp}"
    elif level == "error":
        log = f"ERROR: {function} | {message} | {timestamp}"

    with open(log_file_path, "a") as f:
        f.write(f"{log}\n")
