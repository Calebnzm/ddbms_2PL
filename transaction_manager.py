"""
Transaction Manager for Two-Phase Locking (2PL) Protocol

This module coordinates transactions with the lock manager and node manager.
Implements the full transaction lifecycle with Strict 2PL semantics.
"""

import sqlite3
from typing import Dict, Optional, Any
from lock_manager import LockManager, LockType
from transaction import Transaction, TransactionState
from node_manager import NodeManager


class TransactionManager:
    """
    Orchestrates transactions across distributed database nodes.
    
    Coordinates between LockManager for concurrency control and
    NodeManager for actual data access.
    
    Implements Strict Two-Phase Locking (SS2PL):
    - Growing phase: Acquire locks as needed for reads/writes
    - Shrinking phase: Release all locks only at commit/abort
    """
    
    def __init__(self, node_manager: NodeManager, lock_timeout: float = 10.0):
        """
        Initialize the transaction manager.
        
        Args:
            node_manager: NodeManager instance for data access
            lock_timeout: Maximum time to wait for lock acquisition
        """
        self.node_manager = node_manager
        self.lock_manager = LockManager(lock_timeout=lock_timeout)
        self.active_transactions: Dict[int, Transaction] = {}
        self._connections: Dict[int, Dict[str, sqlite3.Connection]] = {}  # txn_id -> {node_name: conn}
    
    def begin_transaction(self) -> Transaction:
        """
        Begin a new transaction.
        
        Returns:
            A new Transaction object in ACTIVE state
        """
        txn = Transaction()
        self.active_transactions[txn.txn_id] = txn
        self._connections[txn.txn_id] = {}
        print(f"[TxnManager] Started transaction {txn.txn_id}")
        return txn
    
    def _get_connection(self, txn: Transaction, node_name: str) -> sqlite3.Connection:
        """Get or create a connection for a transaction to a specific node"""
        if node_name not in self._connections[txn.txn_id]:
            db_path = self.node_manager.node_files[node_name]
            conn = sqlite3.connect(db_path)
            self._connections[txn.txn_id][node_name] = conn
        return self._connections[txn.txn_id][node_name]
    
    def _close_connections(self, txn: Transaction) -> None:
        """Close all connections for a transaction"""
        if txn.txn_id in self._connections:
            for conn in self._connections[txn.txn_id].values():
                conn.close()
            del self._connections[txn.txn_id]
    
    def execute_read(self, txn: Transaction, account_id: int) -> Optional[int]:
        """
        Execute a read operation within a transaction.
        
        Acquires a SHARED lock on the account, then reads the balance.
        
        Args:
            txn: The transaction
            account_id: Account ID to read
            
        Returns:
            The account balance, or None if account not found
            
        Raises:
            RuntimeError: If transaction is not active or lock cannot be acquired
        """
        if not txn.is_active():
            raise RuntimeError(f"Transaction {txn.txn_id} is not active")
        
        # Find which node has this account
        node_name = self.node_manager.get_node_for_account(account_id)
        if node_name is None:
            print(f"[TxnManager] Txn {txn.txn_id}: Account {account_id} not found")
            return None
        
        # Acquire shared lock
        if not self.lock_manager.acquire_lock(txn.txn_id, node_name, account_id, LockType.SHARED):
            raise RuntimeError(f"Failed to acquire lock on account {account_id}")
        
        txn.add_lock(node_name, account_id, "S")
        
        # Read the balance
        conn = self._get_connection(txn, node_name)
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (account_id,))
        result = cursor.fetchone()
        
        if result:
            balance = result[0]
            txn.record_read(node_name, account_id, balance)
            print(f"[TxnManager] Txn {txn.txn_id} read account {account_id}: balance = {balance}")
            return balance
        return None
    
    def execute_write(self, txn: Transaction, account_id: int, balance: int) -> bool:
        """
        Execute a write operation within a transaction.
        
        Acquires an EXCLUSIVE lock on the account, then buffers the write.
        The actual write is applied at commit time.
        
        Args:
            txn: The transaction
            account_id: Account ID to write to
            balance: New balance value
            
        Returns:
            True if write was buffered successfully
            
        Raises:
            RuntimeError: If transaction is not active or lock cannot be acquired
            ValueError: If balance is negative
        """
        if not txn.is_active():
            raise RuntimeError(f"Transaction {txn.txn_id} is not active")
        
        if balance < 0:
            raise ValueError(f"Balance cannot be negative: {balance}")
        
        # Find which node has this account
        node_name = self.node_manager.get_node_for_account(account_id)
        if node_name is None:
            raise RuntimeError(f"Account {account_id} not found")
        
        # Acquire exclusive lock (may upgrade from shared)
        if not self.lock_manager.acquire_lock(txn.txn_id, node_name, account_id, LockType.EXCLUSIVE):
            raise RuntimeError(f"Failed to acquire exclusive lock on account {account_id}")
        
        txn.add_lock(node_name, account_id, "X")
        
        # Read original value if not already read (for potential rollback)
        if txn.get_original_value(account_id) is None:
            conn = self._get_connection(txn, node_name)
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM accounts WHERE account_id = ?", (account_id,))
            result = cursor.fetchone()
            if result:
                txn.record_read(node_name, account_id, result[0])
        
        # Buffer the write
        txn.buffer_write(node_name, account_id, balance)
        print(f"[TxnManager] Txn {txn.txn_id} buffered write: account {account_id} = {balance}")
        return True
    
    def transfer(
        self, 
        txn: Transaction, 
        from_account: int, 
        to_account: int, 
        amount: int
    ) -> bool:
        """
        Transfer money between two accounts atomically.
        
        This demonstrates cross-account (potentially cross-node) transactions.
        
        Args:
            txn: The transaction
            from_account: Source account ID
            to_account: Destination account ID
            amount: Amount to transfer
            
        Returns:
            True if transfer was successful
            
        Raises:
            RuntimeError: If accounts not found or insufficient balance
            ValueError: If amount is not positive
        """
        if amount <= 0:
            raise ValueError("Transfer amount must be positive")
        
        # Read both balances (acquires shared locks)
        from_balance = self.execute_read(txn, from_account)
        to_balance = self.execute_read(txn, to_account)
        
        if from_balance is None:
            raise RuntimeError(f"Source account {from_account} not found")
        if to_balance is None:
            raise RuntimeError(f"Destination account {to_account} not found")
        
        if from_balance < amount:
            raise RuntimeError(f"Insufficient balance: {from_balance} < {amount}")
        
        # Write new balances (acquires exclusive locks, upgrading from shared)
        self.execute_write(txn, from_account, from_balance - amount)
        self.execute_write(txn, to_account, to_balance + amount)
        
        print(f"[TxnManager] Txn {txn.txn_id} transfer: {amount} from {from_account} to {to_account}")
        return True
    
    def commit_transaction(self, txn: Transaction) -> bool:
        """
        Commit a transaction.
        
        Applies all buffered writes and releases all locks.
        
        Args:
            txn: The transaction to commit
            
        Returns:
            True if commit successful
        """
        if not txn.is_active():
            raise RuntimeError(f"Transaction {txn.txn_id} is not active")
        
        try:
            # Apply all buffered writes
            write_buffer = txn.get_write_buffer()
            for op in write_buffer:
                conn = self._get_connection(txn, op.node_name)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE accounts SET balance = ? WHERE account_id = ?",
                    (op.value, op.account_id)
                )
                conn.commit()
            
            # Mark transaction as committed
            txn.commit()
            
            # Release all locks (shrinking phase)
            self.lock_manager.release_all_locks(txn.txn_id)
            
            # Cleanup
            self._close_connections(txn)
            del self.active_transactions[txn.txn_id]
            
            print(f"[TxnManager] Transaction {txn.txn_id} committed successfully")
            return True
            
        except Exception as e:
            print(f"[TxnManager] Commit failed for txn {txn.txn_id}: {e}")
            self.abort_transaction(txn)
            return False
    
    def abort_transaction(self, txn: Transaction) -> None:
        """
        Abort a transaction.
        
        Discards buffered writes and releases all locks.
        Note: In SS2PL, we don't need to rollback because writes are only
        applied at commit time.
        
        Args:
            txn: The transaction to abort
        """
        if txn.is_committed():
            raise RuntimeError(f"Cannot abort: Transaction {txn.txn_id} is already committed")
        
        # Mark transaction as aborted
        txn.abort()
        
        # Release all locks
        self.lock_manager.release_all_locks(txn.txn_id)
        
        # Cleanup connections
        self._close_connections(txn)
        
        if txn.txn_id in self.active_transactions:
            del self.active_transactions[txn.txn_id]
        
        print(f"[TxnManager] Transaction {txn.txn_id} aborted")
    
    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        """Get a transaction by ID"""
        return self.active_transactions.get(txn_id)
    
    def get_active_transaction_count(self) -> int:
        """Get the number of active transactions"""
        return len(self.active_transactions)
