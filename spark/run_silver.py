from app.services.silver_service import start_silver_stream


def main():

    print("Starting Silver Streaming Job...")

    start_silver_stream()


if __name__ == "__main__":
    main()