from app.services.gold_service import start_gold_job

def main():

    print("Starting Gold Analytics Job...")

    start_gold_job()

    print("Gold Analytics Job completed successfully.")


if __name__ == "__main__":
    main()