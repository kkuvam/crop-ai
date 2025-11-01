#!/bin/bash

# Folder merge script with duplicate handling
# Usage: ./merge_folders.sh <source_folder> <destination_folder> [mode]
# Modes: skip, overwrite, rename, backup, keeplarger

SOURCE="$1"
DEST="$2"
MODE="${3:-skip}"  # Default mode is 'skip'

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Validate arguments
if [ -z "$SOURCE" ] || [ -z "$DEST" ]; then
    echo "Usage: $0 <source_folder> <destination_folder> [mode]"
    echo "Modes:"
    echo "  skip       - Skip duplicate files (default)"
    echo "  overwrite  - Overwrite existing files"
    echo "  rename     - Rename duplicates with timestamp"
    echo "  backup     - Backup existing files before overwrite"
    echo "  keeplarger - Keep the file with larger size"
    exit 1
fi

# Check if source exists
if [ ! -d "$SOURCE" ]; then
    echo -e "${RED}Error: Source folder '$SOURCE' does not exist${NC}"
    exit 1
fi

# Create destination if it doesn't exist
if [ ! -d "$DEST" ]; then
    echo -e "${YELLOW}Creating destination folder: $DEST${NC}"
    mkdir -p "$DEST"
fi

# Counters
copied=0
skipped=0
overwritten=0
renamed=0
larger_kept=0

echo "Starting merge from '$SOURCE' to '$DEST' (mode: $MODE)"
echo "----------------------------------------"

# Find all files in source
while IFS= read -r -d '' file; do
    # Get relative path
    rel_path="${file#$SOURCE/}"
    dest_file="$DEST/$rel_path"
    dest_dir=$(dirname "$dest_file")
    
    # Create destination directory if needed
    mkdir -p "$dest_dir"
    
    # Check if file exists in destination
    if [ -f "$dest_file" ]; then
        case "$MODE" in
            skip)
                echo -e "${YELLOW}SKIP:${NC} $rel_path (already exists)"
                ((skipped++))
                ;;
            overwrite)
                cp -f "$file" "$dest_file"
                echo -e "${GREEN}OVERWRITE:${NC} $rel_path"
                ((overwritten++))
                ;;
            keeplarger)
                # Compare file sizes
                source_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)
                dest_size=$(stat -f%z "$dest_file" 2>/dev/null || stat -c%s "$dest_file" 2>/dev/null)
                
                if [ "$source_size" -gt "$dest_size" ]; then
                    cp -f "$file" "$dest_file"
                    echo -e "${GREEN}REPLACE:${NC} $rel_path (source larger: $source_size > $dest_size bytes)"
                    ((overwritten++))
                elif [ "$source_size" -eq "$dest_size" ]; then
                    echo -e "${YELLOW}SKIP:${NC} $rel_path (same size: $source_size bytes)"
                    ((skipped++))
                else
                    echo -e "${YELLOW}KEEP:${NC} $rel_path (destination larger: $dest_size > $source_size bytes)"
                    ((larger_kept++))
                fi
                ;;
            rename)
                # Add timestamp to filename
                filename=$(basename "$dest_file")
                extension="${filename##*.}"
                basename="${filename%.*}"
                timestamp=$(date +%Y%m%d_%H%M%S)
                
                if [ "$filename" = "$extension" ]; then
                    # No extension
                    new_name="${dest_dir}/${basename}_${timestamp}"
                else
                    new_name="${dest_dir}/${basename}_${timestamp}.${extension}"
                fi
                
                cp "$file" "$new_name"
                echo -e "${GREEN}RENAME:${NC} $rel_path -> $(basename "$new_name")"
                ((renamed++))
                ;;
            backup)
                # Backup existing file
                backup_file="${dest_file}.bak"
                mv "$dest_file" "$backup_file"
                cp "$file" "$dest_file"
                echo -e "${GREEN}BACKUP:${NC} $rel_path (backup created)"
                ((overwritten++))
                ;;
            *)
                echo -e "${RED}Unknown mode: $MODE${NC}"
                exit 1
                ;;
        esac
    else
        # File doesn't exist, just copy
        cp "$file" "$dest_file"
        echo -e "${GREEN}COPY:${NC} $rel_path"
        ((copied++))
    fi
done < <(find "$SOURCE" -type f -print0)

# Summary
echo "----------------------------------------"
echo "Merge complete!"
echo "Files copied: $copied"
echo "Files skipped: $skipped"
echo "Files overwritten: $overwritten"
echo "Files renamed: $renamed"
[ "$MODE" = "keeplarger" ] && echo "Larger files kept in destination: $larger_kept"
echo "Total processed: $((copied + skipped + overwritten + renamed + larger_kept))"