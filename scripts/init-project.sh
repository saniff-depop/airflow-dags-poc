#!/usr/bin/env bash

set -euo pipefail

echo "Initializing project..."
printf "Enter project name: "
read -r PROJECT_NAME_INPUT

printf "Enter project location (parent path): "
read -r PROJECT_LOCATION

# With dashes: lowercase, trim, replace spaces with single dash, collapse multiple dashes
PROJECT_NAME_DASHED=$(printf '%s' "$PROJECT_NAME_INPUT" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/^[[:space:]]+|[[:space:]]+$//g' \
    | tr ' ' '-' \
    | sed -E 's/-+/-/g')

# Snake case: lowercase, non-alphanumeric → single underscore, collapse/trim underscores
PROJECT_NAME_SNAKE=$(printf '%s' "$PROJECT_NAME_INPUT" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/^[[:space:]]+|[[:space:]]+$//g' \
    | sed -E 's/[^a-z0-9]+/_/g' \
    | sed -E 's/_+/_/g; s/^_|_$//g')

TARGET_DIR="${PROJECT_LOCATION%/}/$PROJECT_NAME_DASHED"

echo "Project name with dashes: $PROJECT_NAME_DASHED"
echo "Project name (snake_case): $PROJECT_NAME_SNAKE"
echo "Project parent location: $PROJECT_LOCATION"
echo "Full project path: $TARGET_DIR"

# If target directory exists and is not empty, skip copying
if [ -d "$TARGET_DIR" ] && [ -n "$(ls -A "$TARGET_DIR" 2>/dev/null)" ]; then
    echo "Target directory is not empty. Skipping file copy."
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    TEMPLATE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

    mkdir -p "$TARGET_DIR"

    echo "Copying template files to: $TARGET_DIR"
    rsync -a \
        --exclude '.expo' \
        --exclude '.vscode' \
        --exclude '.git' \
        --exclude 'node_modules' \
        --exclude 'coverage' \
        --exclude 'trash' \
        "$TEMPLATE_ROOT"/ "$TARGET_DIR"/

    if [ -d "$TARGET_DIR/src/python_poetry_template" ] && [ "python_poetry_template" != "$PROJECT_NAME_SNAKE" ]; then
        echo "Renaming src/python_poetry_template to src/$PROJECT_NAME_SNAKE"
        mv "$TARGET_DIR/src/python_poetry_template" "$TARGET_DIR/src/$PROJECT_NAME_SNAKE"
    fi
fi

PYPROJECT_TOML="$TARGET_DIR/pyproject.toml"
if [ -f "$PYPROJECT_TOML" ]; then
  sed -E -i.bak \
    -e "s/python-poetry-template/$PROJECT_NAME_DASHED/g" \
    -e "s/python_poetry_template/$PROJECT_NAME_SNAKE/g" \
    "$PYPROJECT_TOML"
  rm -f "${PYPROJECT_TOML}.bak"
fi

TEST_MAIN_PY="$TARGET_DIR/tests/unit/test_main.py"
if [ -f "$TEST_MAIN_PY" ]; then
  sed -E -i.bak \
    -e "s/python_poetry_template/$PROJECT_NAME_SNAKE/g" \
    "$TEST_MAIN_PY"
  rm -f "${TEST_MAIN_PY}.bak"
fi

echo "Project initialized successfully!"
