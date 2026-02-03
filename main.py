from node_manager import NodeManager
from transaction_manager import TransactionManager
from transaction import Transaction, TransactionType
import os

def main():
    print("Hello from project!")
    
    # Path to the config file
    config_path = os.path.abspath("fragmentation.toml")
    
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        return

    # Initialize NodeManager
    nm = NodeManager(config_path)
    
    # Load accounts from CSV
    print("\n--- Loading Accounts from CSV ---")
    csv_file = "all_accounts.csv"
    csv_path = os.path.abspath(csv_file)
    if os.path.exists(csv_path):
        print(f"Loading accounts from {csv_file}...")
        nm.add_accounts_from_csv(csv_path)
    else:
        print(f"Warning: CSV file {csv_file} not found.")

    # Initialize TransactionManager
    tm = TransactionManager(nm)

    # Test Transaction 1: Deposit (Deposit 2500 to 1001)
    print("\n--- Testing Transaction 1: Deposit ---")
    txn1 = Transaction(
        txn_type=TransactionType.DEPOSIT,
        args={"account_id": 1001, "amount": 2500}
    )
    tm.execute_transaction(txn1)
    
    # Verify result (Initial 5000 + 2500 = 7500)
    print(f"New Balance for 1001: {nm.read_balance(1001)}")

    # Test Transaction 2: Transfer (Transfer 500 from 1001 to 3001)
    print("\n--- Testing Transaction 2: Transfer ---")
    txn2 = Transaction(
        txn_type=TransactionType.TRANSFER,
        args={"from_account": 1001, "to_account": 3001, "amount": 500}
    )
    tm.execute_transaction(txn2)
    
    # Verify results (1001: 7500 - 500 = 7000, 3001: 1000 + 500 = 1500)
    print(f"Final Balance for 1001: {nm.read_balance(1001)}")
    print(f"Final Balance for 3001: {nm.read_balance(3001)}")

if __name__ == "__main__":
    main()
