#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 -i IP_TO_REMOVE -p PATH_TO_LOGS"
    exit 1
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to validate IP address format
validate_ip() {
    local ip=$1
    local valid=1

    if [[ $ip =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
        for octet in $(echo $ip | tr '.' ' '); do
            if ((octet < 0 || octet > 255)); then
                valid=0
                break
            fi
        done
    else
        valid=0
    fi

    if [ $valid -eq 0 ]; then
        echo "Invalid IP address format: $ip"
        exit 1
    fi
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
    rm -f "$temp_file"
    log "Cleaned up temporary files"
}

# Trap signals for cleanup
trap cleanup EXIT INT TERM

# Check if the OS is Linux
if [[ "$(uname)" != "Linux" ]]; then
    echo "This script can only be run on Linux."
    exit 1
fi

# Parse command line arguments
while getopts ":i:p:v" opt; do
  case ${opt} in
    i )
      IP_TO_REMOVE=$OPTARG
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

# Check if IP_TO_REMOVE and PATH_TO_LOGS are set
if [ -z "$IP_TO_REMOVE" ] || [ -z "$PATH_TO_LOGS" ]; then
    usage
fi

# Validate the IP address format
validate_ip "$IP_TO_REMOVE"

# Check required commands
for cmd in find zcat grep gzip mktemp mv cp; do
    if ! command_exists "$cmd"; then
        echo "Command not found: $cmd"
        exit 1
    fi
done

# Check write permissions
check_write_permission "$PATH_TO_LOGS"

# Set log file path
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/process_logs_$(date '+%Y%m%d_%H%M%S').log"
mkdir -p "$LOG_DIR" || { echo "Failed to create log directory"; exit 1; }
touch "$LOG_FILE" || { echo "Failed to create log file"; exit 1; }

# Find and count the number of .gz files
file_count=$(find "$PATH_TO_LOGS" -type f -name '*.gz' | wc -l)

# Prompt the user for confirmation
echo "Found $file_count .gz files in $PATH_TO_LOGS."
read -p "Do you want to proceed with removing IP $IP_TO_REMOVE from these files? (y/n): " confirm

if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Operation cancelled."
    exit 0
fi

# Process each file
find "$PATH_TO_LOGS" -type f -name '*.gz' | while read -r file; do
    # Create a backup of the original file
    backup_file="${file}.bak"
    cp "$file" "$backup_file" || { log "Failed to create backup for $file"; continue; }
    log "Created backup for $file"

    # Use a temporary file to store the filtered output
    temp_file=$(mktemp) || { log "Failed to create temporary file"; continue; }

    # Decompress, filter, and recompress the log file
    if zcat "$file" | grep -v -F "$IP_TO_REMOVE" | gzip > "$temp_file"; then
        # Replace the original file with the filtered one
        mv "$temp_file" "$file" || { log "Failed to replace original file $file"; rm -f "$temp_file"; continue; }
        log "Processed $file"
    else
        log "Error processing $file"
        rm -f "$temp_file"
    fi
done

# Cleanup: Optionally remove backup files after successful processing
read -p "Do you want to remove the backup files? (y/n): " remove_backup_files
if [[ "$remove_backup_files" == "y" || "$remove_backup_files" == "Y" ]]; then
    find "$PATH_TO_LOGS" -type f -name '*.gz.bak' -exec rm -f {} \;
    log "Backup files removed"
fi

