import subprocess
import sys


def main():
    """Run the Observable Framework dev server."""
    subprocess.run(["npm", "run", "dev"], check=True)


if __name__ == "__main__":
    main()
