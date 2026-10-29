"""Command-line entry point for the Kafka-to-Bronze streaming job."""

from app.services.streaming_service import start_stream


def main():
    """Start the streaming query and block until it is stopped."""

    print("Starting Spark Streaming Consumer...")

    start_stream()


if __name__ == "__main__":
    main()
