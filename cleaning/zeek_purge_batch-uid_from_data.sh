#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 -u UID_LIST_FILE -p PATH_TO_LOGS"
    exit 1
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1 || { echo "Error: $1 is not installed."; exit 1; }
}

# Function to check if write permission is available
check_write_permission() {
    local dir=$1
    if [ ! -w "$dir" ]; then
        echo "No write permission in directory: $dir"
        exit 1
    fi
}

# Function to log messages
log() {
    local message=$1
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - $message" | tee -a "$LOG_FILE"
}

# Function to clean up temporary files
cleanup() {
    if [ -n "$temp_file" ] && [ -f "$temp_file" ]; then
        rm -f "$temp_file"
        log "Cleaned up temporary files"
    fi
}

# Trap signals for cleanup
trap cleanup EXIT INT TERM

# Check if the OS is Linux
if [[ "$(uname)" != "Linux" ]]; then
    echo "This script can only be run on Linux."
    exit 1
fi

# Parse command line arguments
while getopts ":u:p:v" opt; do
  case ${opt} in
    u )
      UID_LIST_FILE=$OPTARG
      ;;
    p )
      PATH_TO_LOGS=$OPTARG
      ;;
    v )
      VERBOSE=1
      ;;
    \? )
      usage
      ;;
  esac
done

# Check if UID_LIST_FILE and PATH_TO_LOGS are set
if [ -z "$UID_LIST_FILE" ] || [ -z "$PATH_TO_LOGS" ]; then
    usage
fi

# Check required commands
for cmd in find zcat grep gzip mktemp mv cp; do
    command_exists "$cmd"
done

# Check write permissions
check_write_permission "$PATH_TO_LOGS"

# Validate UID list file
if [ ! -f "$UID_LIST_FILE" ]; then
    echo "UID list file not found: $UID_LIST_FILE"
    exit 1
fi

# Set log file path
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/process_logs_$(date '+%Y%m%d_%H%M%S').log"
mkdir -p "$LOG_DIR" || { echo "Failed to create log directory"; exit 1; }
touch "$LOG_FILE" || { echo "Failed to create log file"; exit 1; }

# Read UIDs from file into an array
readarray -t UIDs < "$UID_LIST_FILE"

# Find and count the number of .gz files
file_count=$(find "$PATH_TO_LOGS" -type f -name '*.gz' | wc -l)

# Prompt the user for confirmation
echo "Found $file_count .gz files in $PATH_TO_LOGS."
read -p "Do you want to proceed with removing UIDs from the list in these files? (y/n): " confirm

if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Operation cancelled."
    exit 0
fi

# Process each file for each UID
find "$PATH_TO_LOGS" -type f -name '*.gz' | while read -r file; do
    # Create a backup of the original file
    backup_file="${file}.bak"
    cp "$file" "$backup_file" || { log "Failed to create backup for $file"; continue; }
    log "Created backup for $file"

    # Use a temporary file to store the filtered output
    temp_file=$(mktemp) || { log "Failed to create temporary file"; continue; }

    # Decompress the log file
    zcat "$file" > "$temp_file"

    # Remove each UID strictly
    for uid in "${UIDs[@]}"; do
        grep -v -F "$uid" "$temp_file" > "${temp_file}.filtered"
        mv "${temp_file}.filtered" "$temp_file"
    done

    # Recompress the log file
    gzip < "$temp_file" > "$file"

    # Log the processing
    log "Processed $file"

    # Cleanup temporary file
    rm -f "$temp_file"
done

# Cleanup: Optionally remove backup files after successful processing
read -p "Do you want to remove the backup files? (y/n): " remove_backup_files
if [[ "$remove_backup_files" == "y" || "$remove_backup_files" == "Y" ]]; then
    find "$PATH_TO_LOGS" -type f -name '*.gz.bak' -exec rm -f {} \;
    log "Backup files removed"
fi

