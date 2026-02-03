from node_manager import NodeManager
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

    # Test reading balance
    print("\n--- Testing Read Balance ---")
    print(f"Balance for 1001 (Kisumu): {nm.read_balance(1001)}")
    print(f"Balance for 2001 (Nairobi): {nm.read_balance(2001)}")
    print(f"Balance for 3001 (Mombasa): {nm.read_balance(3001)}")
    
    # Test writing balance
    print("\n--- Testing Write Balance ---")
    nm.write_balance(1001, 5000)
    print(f"New Balance for 1001: {nm.read_balance(1001)}")
    
    # Test deleting account
    print("\n--- Testing Delete Account ---")
    nm.delete_account(2001)
    print(f"Balance for 2001 (should be None): {nm.read_balance(2001)}")

if __name__ == "__main__":
    main()
