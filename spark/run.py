from app.services.streaming_service import start_stream


def main():

    print("Starting Spark Streaming Consumer...")

    start_stream()


if __name__ == "__main__":
    main()