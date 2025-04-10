import os


def load_seed_urls(file_path='data/seed_urls.txt'):
    """
    Loads seed URLs from a text file.

    Args:
        file_path (str): Relative or absolute path to the seed URLs file.

    Returns:
        list[str]: A list of cleaned, non-empty URLs.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Seed URL file not found at {file_path}")

    with open(file_path, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    return urls
