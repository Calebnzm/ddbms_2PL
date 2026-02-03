# NodeManager

`NodeManager` is a Python class designed to manage distributed SQLite database nodes. It handles account creation, deletion, and balance operations across multiple database files based on a geographic configuration.

## Features

- **Distributed Storage**: Manages multiple SQLite database nodes defined in a configuration file.
- **Geographic Fragmentation**: Routes accounts to specific nodes based on the city/county of the account holder.
- **Global Account Index**: Maintains an in-memory index of `account_id` to `node_name` for fast O(1) lookups.
- **Automatic ID Generation**: Auto-increments global account IDs to ensure uniqueness across all nodes.
- **Bulk Import**: Supports importing accounts from CSV files.

## Configuration

The system relies on a TOML configuration file (e.g., `fragmentation.toml`) that defines the nodes and their associated counties.

### Example `fragmentation.toml`

```toml
[[nodes]]
name = "kisumu"
db_path = "database_nodes/kisumu.db"
counties = ["Kisumu", "Siaya", "Homa Bay", ...]

[[nodes]]
name = "nairobi"
db_path = "database_nodes/nairobi.db"
counties = ["Nairobi", "Kiambu", "Murang'a", ...]
```

## Usage

### Initialization

Initialize `NodeManager` by passing the path to the configuration file.

```python
from node_manager import NodeManager

nm = NodeManager("fragmentation.toml")
```

Upon initialization, it will:
1. Load the configuration.
2. Ensure all database files and tables exist.
3. Build an in-memory index of all existing accounts.

### 1. Create an Account

Creates a new account in the appropriate node based on the city. Returns the assigned `account_id`.

```python
# Create an account in Kisumu (routed to kisumu.db)
account_id = nm.create_account("Kisumu", initial_balance=5000)
print(f"Created account {account_id}")
```

### 2. Read Balance

Retrieves the balance for a given valid `account_id`.

```python
balance = nm.read_balance(account_id)
if balance is not None:
    print(f"Balance: {balance}")
else:
    print("Account not found")
```

### 3. Write Balance

Updates the balance for an account.

```python
nm.write_balance(account_id, 7500)
```

### 4. Delete Account

Removes the account from the database and the in-memory index.

```python
nm.delete_account(account_id)
```

### 5. Import from CSV

Bulk creates accounts from a CSV file. The CSV must have a `city` column and optionally a `balance` column.

**CSV Format:**
```csv
city,balance
Kisumu,1000
Nairobi,2000
Mombasa,1500
```

**Code:**
```python
nm.add_accounts_from_csv("accounts.csv")
```

## Internal Methods

- **`get_node_for_city(city)`**: Returns the node name responsible for the given city based on the config.
- **`get_node_for_account(account_id)`**: Returns the node name where the account resides using the in-memory index.

# Transaction Management

The system implements **Strict Two-Phase Locking (SS2PL)** to ensure serializability and data consistency across distributed nodes.

## Usage

Transactions are defined declaratively using `TransactionType` and executed by `TransactionManager`.

### 1. Transfer
Transfers an amount between two accounts atomically.
```python
from transaction_manager import TransactionManager
from transaction import Transaction, TransactionType

tm = TransactionManager(nm)

txn = Transaction(
    txn_type=TransactionType.TRANSFER,
    args={"from_account": 1001, "to_account": 3001, "amount": 500}
)

tm.execute_transaction(txn)
```

### 2. Deposit
Deposits money into an account.
```python
txn = Transaction(
    txn_type=TransactionType.DEPOSIT,
    args={"account_id": 1001, "amount": 1000}
)
tm.execute_transaction(txn)
```

### 3. Withdraw
Withdraws money from an account (fails if insufficient funds).
```python
txn = Transaction(
    txn_type=TransactionType.WITHDRAW,
    args={"account_id": 1001, "amount": 200}
)
tm.execute_transaction(txn)
```

## How It Works

1.  **High-Level Definition**: You define *what* you want to do (e.g., "Transfer 500").
2.  **Resolution**: `TransactionManager` resolves this into specific **READ** and **WRITE** operations.
3.  **Locking (2PL)**:
    *   **Reads** acquire **Shared (S)** locks.
    *   **Writes** acquire **Exclusive (X)** locks.
    *   Locks are held until the transaction commits or aborts.
4.  **Execution**: `NodeManager` performs the physical database updates.
