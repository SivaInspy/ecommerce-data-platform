"""Command-line entry point for the Silver file-streaming job."""

from app.services.silver_service import start_silver_stream


def main():
    """Start the Silver streaming query and block until it is stopped."""

    print("Starting Silver Streaming Job...")

    start_silver_stream()


if __name__ == "__main__":
    main()
