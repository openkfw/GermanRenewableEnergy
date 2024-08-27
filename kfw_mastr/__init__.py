import argparse
from pathlib import Path

print("Initialisation of the programme. Takes less than 15 seconds")

# remove logfile to allow creation of clean logfile for next run. Run-specific log is copied into output folder with version and outfile_postfix.
# all logifles are appended for debugging in kfw-mastr_debug.log
print("Preparing an empty log file...")
parent_dir = Path(__file__).resolve().parent.parent
log_file_path = parent_dir / 'logs' / 'kfw-mastr.log'
# Remove the log file
try:
    log_file_path.unlink()
    print("Old log files cleaned. Continuing initialisation")
except:
    print("No old log files found. Continuing initialisation")

# Module-level variable to store config path
config_path = None

def parse_args():
    global config_path
    parser = argparse.ArgumentParser(description="Control calculations with help of config file")
    parser.add_argument('--config_path', default='config.yaml', type=str, help='Path to the config.yaml file')
    args, unknown = parser.parse_known_args()
    config_path = args.config_path

# Parse arguments when the module is imported
parse_args()