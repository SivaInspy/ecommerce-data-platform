"""Command-line entry point for building Gold analytics."""

from app.services.gold_service import start_gold_job

def main():
    """Run the Gold batch job."""

    print("Starting Gold Analytics Job...")

    start_gold_job()

    print("Gold Analytics Job completed successfully.")


if __name__ == "__main__":
    main()
