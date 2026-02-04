import sqlite3
import os
import pandas as pd
import tomllib
from typing import Dict, Optional
from logger_config import setup_logger

logger = setup_logger(__name__)

class NodeManager:
    def __init__(self, config_file: str):
        with open(config_file, "rb") as f:
            self.config = tomllib.load(f)
        self.node_files = {node["name"]: node["db_path"] for node in self.config["nodes"]}
        self.global_account_id = 1
        self.account_index: Dict[int, str] = {}
        self._initialize_nodes()
        self._build_index()

    def _initialize_nodes(self):
        for node_name, db_file in self.node_files.items():
            new_db = not os.path.exists(db_file)
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id INTEGER PRIMARY KEY,
                    city TEXT,
                    balance INTEGER CHECK(balance >= 0)
                )
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_account_id ON accounts(account_id)
            """)
            conn.commit()
            conn.close()
            if new_db:
                logger.info(f"Node '{node_name}' created at {db_file}.")

    def _build_index(self):
        """Builds in-memory account_id -> node_name index"""
        self.account_index.clear()
        for node_name, db_file in self.node_files.items():
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT account_id FROM accounts")
            for (account_id,) in cursor.fetchall():
                self.account_index[account_id] = node_name
                self.global_account_id = max(self.global_account_id, account_id + 1)
            conn.close()
        logger.info(f"In-memory account index built. Next account ID: {self.global_account_id}")

    def create_account(self, city: str, initial_balance: int = 1000, account_id: int = None) -> int:
        """Create a new account with a unique global ID"""
        node_name = self.get_node_for_city(city)
        if node_name is None:
            raise ValueError(f"No node found for city '{city}'.")
        
        if account_id is None:
            account_id = self.global_account_id
            self.global_account_id += 1
        elif account_id >= self.global_account_id:
            # Update global counter if we import a higher ID
            self.global_account_id = account_id + 1

        conn = sqlite3.connect(self.node_files[node_name])
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO accounts (account_id, city, balance) VALUES (?, ?, ?)",
            (account_id, city, initial_balance)
        )
        conn.commit()
        conn.close()

        self.account_index[account_id] = node_name
        logger.info(f"Account '{account_id}' added to node '{node_name}' with balance {initial_balance}")
        return account_id

    def delete_account(self, account_id: int) -> None:
        node_name = self.get_node_for_account(account_id)
        if node_name is None:
            logger.warning(f"No node found for account '{account_id}'")
            return
        conn = sqlite3.connect(self.node_files[node_name])
        cursor = conn.cursor()
        cursor.execute("DELETE FROM accounts WHERE account_id = ?", (account_id,))
        conn.commit()
        conn.close()
        self.account_index.pop(account_id, None)
        logger.info(f"Account '{account_id}' deleted from node '{node_name}'")

    def read_balance(self, account_id: int) -> Optional[int]:
        node_name = self.get_node_for_account(account_id)
        if node_name is None:
            logger.warning(f"No node found for account '{account_id}'")
            return None
        conn = sqlite3.connect(self.node_files[node_name])
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (account_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def write_balance(self, account_id: int, balance: int) -> None:
        node_name = self.get_node_for_account(account_id)
        if node_name is None:
            logger.warning(f"No node found for account '{account_id}'")
            return
        if balance < 0:
            raise ValueError(f"Balance cannot be negative: {balance}")
        conn = sqlite3.connect(self.node_files[node_name])
        cursor = conn.cursor()
        cursor.execute("UPDATE accounts SET balance = ? WHERE account_id = ?", (balance, account_id))
        conn.commit()
        conn.close()
        logger.info(f"Account '{account_id}' balance updated to {balance} on node '{node_name}'")

    def add_accounts_from_csv(self, csv_file: str) -> None:
        accounts_df = pd.read_csv(csv_file)
        for _, row in accounts_df.iterrows():
            # Check if account_id exists in CSV
            acc_id = int(row['account_id']) if 'account_id' in row else None
            self.create_account(row['city'], int(row.get('balance', 1000)), account_id=acc_id)

    def get_node_for_city(self, city: str) -> Optional[str]:
        for node in self.config['nodes']:
            if city in node['counties']:
                return node['name']
        return None

    def get_node_for_account(self, account_id: int) -> Optional[str]:
        return self.account_index.get(account_id)
