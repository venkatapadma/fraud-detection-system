

# Fraud Transaction Detection

This project detects potentially fraudulent transactions using Apache Kafka Streams.

## Fraud Detection Rules

A transaction is flagged as **fraudulent** if it satisfies **any** of the following conditions:

## Technologies Used

- Java
- Apache Kafka
- Apache Kafka Streams
- Maven

---

### 1. High-Value Transaction

- Transaction amount is greater than **10,000**.

**Rule**

```text
amount > 10000
```

**Example**

| Transaction ID | User | Amount | Status |
|----------------|------|---------|--------|
| TXN001 | Alice | 12,500 | Fraud |
| TXN002 | Bob | 8,000 | Valid |


### 2. Multiple Transactions Within 10 Seconds

If a user performs **more than 3 transactions within a 10-second window**, the transactions are considered suspicious.

**Rule**

```text
transactions_per_user > 3 within 10 seconds
```

**Example**

| Time | User | Transaction |
|------|------|-------------|
| 10:00:01 | Alice | ₹500 |
| 10:00:03 | Alice | ₹700 |
| 10:00:06 | Alice | ₹300 |
| 10:00:08 | Alice | ₹450 |

Since Alice performed **4 transactions in 10 seconds**, the rule is triggered.

---

## Processing Flow

```
Incoming Transactions
        │
        ▼
Kafka Topic
        │
        ▼
Kafka Streams Application
        │
        ├── Rule 1: Amount > 10000
        │
        └── Rule 2: More than 3 transactions in 10 seconds
        │
        ▼
Fraudulent Transactions
        │
        ▼
Output Kafka Topic
```

## Output

Transactions that match either fraud rule are published to the configured fraud output topic for further processing, alerting, storage.

## Features
- branching transactions based on credit and debit
- Location-based fraud detection
