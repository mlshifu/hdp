#!/bin/bash

# Variables
REMOTE_USER="your_username"      # Replace with your remote username
REMOTE_HOST="remote.server.com"   # Replace with your remote server address
REMOTE_DIR="/path/to/remote/dir"  # Replace with the remote directory where you want to upload the file
LOCAL_FILE="/path/to/local/file"   # Replace with the local file you want to upload

# Use SFTP to transfer the file
sftp "$REMOTE_USER@$REMOTE_HOST" << EOF
put "$LOCAL_FILE" "$REMOTE_DIR"
bye
EOF

echo "File $LOCAL_FILE has been transferred to $REMOTE_HOST:$REMOTE_DIR"
