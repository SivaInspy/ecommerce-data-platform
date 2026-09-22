# Data Deduplication Strategy

## Real-Time E-Commerce Data Platform

## 1. Purpose

This document describes the deduplication strategy implemented in the Silver layer of the Real-Time E-Commerce Data Platform.

The objective is to:

- Remove exact duplicate records
- Preserve distinct business records
- Avoid incorrectly treating `order_id` as a globally unique identifier
- Make the deduplication behavior explicit and reproducible

---

## 2. Initial Problem

During Bronze-to-Silver processing, duplicate records were identified in the Bronze dataset.

The Bronze dataset contained:

```text
34 total records
```

After data-quality validation:

```text
28 valid records
2 invalid records
```

The invalid records belonged to:

```text
order_id = 102
order_id = 103
```

and were moved to the quarantine layer.

---

## 3. Duplicate Investigation

A duplicate investigation was performed against the Bronze dataset.

The investigation showed that several order IDs appeared multiple times with identical data.

For example, orders 1 through 5 contained repeated copies of the same logical record.

However, order `101` required special attention.

Two different valid records existed for the same order ID:

```text
Order ID:       101
Customer ID:    201
Customer:       Test Valid
City:           Chennai
Product ID:     P101
Product:        Test Phone
Category:       Mobile
Price:          50000
Payment Method: UPI
Order Time:     2026-09-08 13:00
```

and:

```text
Order ID:       101
Customer ID:    202
Customer:       Test Duplicate
City:           Mumbai
Product ID:     P102
Product:        Another Phone
Category:       Mobile
Price:          60000
Payment Method: Credit Card
Order Time:     2026-09-08 13:01
```

These are different records even though they share the same `order_id`.

---

## 4. Initial Deduplication Logic

The original Silver transformation used:

```python
.dropDuplicates(["order_id"])
```

This approach assumes that:

```text
order_id = unique business identifier
```

For the current dataset, that assumption is not valid.

Using:

```python
.dropDuplicates(["order_id"])
```

would cause Spark to retain only one record for order `101`.

As a result, one legitimate business record would be removed.

---

## 5. Revised Deduplication Strategy

The Silver transformation was changed to:

```python
.dropDuplicates()
```

This performs exact-row deduplication.

In other words, Spark removes a row only when the complete row is duplicated.

The strategy is therefore:

```text
Exact duplicate
        |
        v
      Remove

Different record with same order_id
        |
        v
       Keep
```

---

## 6. Why Exact-Row Deduplication Was Selected

The current source schema does not contain a globally unique event identifier such as:

```text
event_id
transaction_id
message_id
```

It also does not currently define a complete business key for identifying unique order events.

Therefore, using:

```python
.dropDuplicates(["order_id"])
```

would make an assumption about business semantics that the source data does not guarantee.

Exact-row deduplication is safer for this portfolio implementation because it only removes records that are identical across all available columns.

---

## 7. Result After the Fix

After changing the deduplication logic, the Silver batch produced:

```text
Bronze record count: 34
Valid records: 28
Invalid records: 2
Silver record count: 7
```

The seven distinct valid records are:

```text
Order 1
Order 2
Order 3
Order 4
Order 5
Order 101 / Customer 201
Order 101 / Customer 202
```

Therefore:

```text
5 original unique orders
+
2 distinct order-101 records
=
7 Silver records
```

---

## 8. Invalid Records

The following records are excluded from Silver because they fail data-quality validation.

### Order 102

```text
Price = -100
```

Validation result:

```text
INVALID_PRICE
```

### Order 103

```text
Customer ID = NULL
```

Validation result:

```text
MISSING_CUSTOMER_ID
```

These records are written to the quarantine layer.

---

## 9. Silver Processing Flow

The final Silver processing sequence is:

```text
Bronze
  |
  v
Data Quality Validation
  |
  +----------------------+
  |                      |
  v                      v
Valid                  Invalid
  |                      |
  v                      v
Deduplication          Quarantine
  |
  v
Transformation
  |
  v
Silver
```

---

## 10. Implementation

The relevant Silver transformation is:

```python
def transform_orders(df: DataFrame) -> DataFrame:
    transformed_df = (
        df
        .withColumn(
            "order_time",
            to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss")
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("price") > 0)
        .dropDuplicates()
    )

    return transformed_df
```

---

## 11. Why `order_id` Is Not Used Alone

An important data-engineering principle is:

> A field should only be treated as a unique identifier when the source-system business contract guarantees its uniqueness.

An `order_id` may represent:

- An order
- Multiple order events
- An order update
- An order status change
- Multiple source-system records
- A business transaction containing multiple events

The correct interpretation depends on the source-system contract.

Therefore, deduplication should be based on documented business semantics rather than assumption.

---

## 12. Production Recommendation

The current strategy is appropriate for the portfolio dataset, but a production implementation should preferably introduce a stable event identifier.

For example:

```text
event_id
```

could uniquely identify each source event.

The deduplication could then become:

```python
.dropDuplicates(["event_id"])
```

Alternatively, if the business defines a composite key, the key could be explicitly documented.

For example:

```text
(order_id, event_type, event_time)
```

or another combination defined by the source-system contract.

The correct key must be determined from the actual business semantics.

---

## 13. Streaming Consideration

For a production Structured Streaming pipeline, deduplication also needs to consider:

- Late-arriving records
- Event-time semantics
- Watermarks
- State-store size
- Restart behavior
- Checkpoint recovery
- Retention requirements

For example, a streaming implementation may use an event-time watermark together with a unique event identifier.

The exact implementation should depend on the source system and expected event lateness.

---

## 14. Data Quality vs Deduplication

Deduplication and data-quality validation are separate concerns.

### Data Quality

Determines whether a record is valid.

Examples:

```text
Missing order_id
Missing customer_id
Missing price
Invalid price
Missing order_time
```

### Deduplication

Determines whether multiple records represent the same record.

These should not be treated as the same operation.

The current pipeline therefore separates:

```text
Validation
    |
    v
Valid / Invalid
    |
    v
Deduplication
```

---

## 15. Validation Metrics

After the deduplication correction, the expected Silver dataset contains:

| Metric | Expected Value |
|---|---:|
| Bronze records | 34 |
| Valid records | 28 |
| Invalid records | 2 |
| Silver records | 7 |

The Gold layer should subsequently calculate metrics from these seven Silver records.

Expected overall Gold metrics are:

| Metric | Expected Value |
|---|---:|
| Total orders | 7 |
| Total revenue | 449,998 |
| Average order value | 64,285.428571... |

---

## 16. Key Interview Takeaway

The important engineering decision is not simply:

```python
.dropDuplicates()
```

The important decision is understanding **what constitutes a duplicate**.

A strong data-engineering implementation should answer:

1. What is the business key?
2. Is the key guaranteed to be unique?
3. Can multiple events share the same order ID?
4. What makes two records identical?
5. How should late-arriving records be handled?
6. How should duplicate events be handled after a job restart?
7. What is the retention period for deduplication state?
8. How is the deduplication behavior validated?

For this project, the source schema does not provide a unique event ID, so exact-row deduplication was selected rather than assuming that `order_id` is globally unique.

---

## 17. Final Decision

The final Silver deduplication strategy is:

```python
.dropDuplicates()
```

because it:

- Removes exact duplicate records
- Preserves distinct records sharing the same `order_id`
- Avoids an unsupported uniqueness assumption
- Produces the expected seven Silver records
- Keeps the implementation simple and deterministic for the current dataset

For a production implementation, the preferred enhancement is to introduce a source-generated unique event identifier or a formally defined business key.
