from pathlib import Path

def find_file_in_prefix(target_name: str, extension: str, prefix_path: Path) -> Path:
    """
    Search for a file with the given name and extension under a specified directory prefix.

    This function recursively searches within the provided `prefix_path` for a file named
    `{target_name}.{extension}`. It returns the full path to the file if found.

    Args:
        target_name (str): The base name of the file (without extension), e.g., 'account_test'.
        extension (str): The file extension (without the dot), e.g., 'yaml'.
        prefix_path (Path): The root directory to begin the search.

    Returns:
        Path: The full path to the matched file.

    Raises:
        FileNotFoundError: If the specified file is not found within the given directory.
    """
    pattern = f"{target_name}.{extension}"
    for path in prefix_path.rglob(pattern):
        if path.is_file():
            return path

    raise FileNotFoundError(f"File '{pattern}' not found under '{prefix_path}'")
