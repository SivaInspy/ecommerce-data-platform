# Kafka order producer

This package directory is reserved for a modular producer implementation; its Python files are empty placeholders. The working sample publisher is the repository-level [`producer.py`](../producer.py). From the repository root, install `requirements.txt` and run:

```powershell
python producer.py
```

It sends five fixed records to topic `orders` at `localhost:9092`, waiting two seconds between messages. Start Kafka and the Spark Bronze stream first. The root Compose stack advertises `kafka:29092` to clients on its internal network. Payloads conform to the [Spark order schema](../docs/ARCHITECTURE.md#order-schema).
