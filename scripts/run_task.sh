#!/bin/bash
is_absolute_path() {
    case "$1" in
        /*) return 0 ;; # Absolute path
        *) return 1 ;;  # Not an absolute path
    esac
}

resolve_path() {
    local path=$1
    if ! is_absolute_path "$path"; then
        path=$(realpath "$bash_path/$path")
    fi
    echo "$path"
}

construct_command() {
    task_path=$(resolve_path "$1")
    if [ -n "$2" ]; then
        out_path=$(resolve_path "$2")
        echo "python \"$script_path\" osiris \"$task_path\" -o \"$out_path\""
    else
        echo "python \"$script_path\" osiris \"$task_path\""
    fi
}

# check if task path is provided, if not exit
if [ -z "$1" ]; then
    echo "Usage: $0 <task_path> [out_path]"
    exit 1
fi

# set paths (based on directory of this bash script)
bash_path=$(dirname "$0")
env_path="$bash_path/../.env"
script_path="$bash_path/run_task.py"

# activate env (first activate bash hooks)
eval "$(conda shell.bash hook)"
conda activate osiris_query_2

# set pythonpath (extract from .env file and export it)
pythonpath_line=$(grep '^PYTHONPATH=' "$env_path")
eval "export $pythonpath_line"

# run script
command=$(construct_command "$@")
eval "$command"
