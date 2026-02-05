"""
Transaction Manager for Two-Phase Locking (2PL) Protocol

This module coordinates transactions with the lock manager and node manager.
Implements the full transaction lifecycle with Strict 2PL semantics.
"""


import random
import time
from typing import Dict, Optional, Any
from lock_manager import LockManager, LockType, DeadlockException
from transaction import Transaction, TransactionState, OperationType, TransactionType
from transaction import Transaction, TransactionState, OperationType, TransactionType
from node_manager import NodeManager
from logger_config import setup_logger

logger = setup_logger(__name__)


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
    
    def begin_transaction(self) -> Transaction:
        """
        Start a new transaction.
        
        Returns:
            The new transaction object
        """
        txn = Transaction()
        self.active_transactions[txn.txn_id] = txn
        self.active_transactions[txn.txn_id] = txn
        logger.info(f"[TxnManager] Started transaction {txn.txn_id}")
        return txn

    def execute_transaction(self, txn: Transaction) -> bool:
        """
        Execute a transaction based on its high-level type.
        
        Args:
            txn: The transaction to execute
            
        Returns:
            True if committed successfully, False if aborted
        """
        self.active_transactions[txn.txn_id] = txn
        logger.info(f"[TxnManager] Started transaction {txn.txn_id} ({txn.txn_type.value})")
        
        max_retries = 3
        for attempt in range(max_retries):
            self.active_transactions[txn.txn_id] = txn
            try:
                if attempt > 0:
                     logger.info(f"[TxnManager] Retrying transaction {txn.txn_id} (Attempt {attempt+1}/{max_retries})")
                
                if txn.txn_type == TransactionType.TRANSFER:
                    self._resolve_transfer(txn)
                elif txn.txn_type == TransactionType.WITHDRAW:
                    self._resolve_withdraw(txn)
                elif txn.txn_type == TransactionType.DEPOSIT:
                    self._resolve_deposit(txn)
                else:
                    raise ValueError(f"Unknown transaction type: {txn.txn_type}")
                
                return self.commit_transaction(txn)
            
            except DeadlockException as e:
                logger.warning(f"[TxnManager] Deadlock detected for txn {txn.txn_id}: {e}")
                self.abort_transaction(txn)
                
                if attempt < max_retries - 1:
                    backoff = random.uniform(0.1, 0.5)
                    logger.info(f"[TxnManager] Backing off for {backoff:.2f}s before retry...")
                    time.sleep(backoff)
                    txn.reset()
                    continue
                else:
                    logger.error(f"[TxnManager] Max retries reached for txn {txn.txn_id}")
                    return False
                
            except Exception as e:
                logger.error(f"[TxnManager] Execution failed for txn {txn.txn_id}: {e}")
                self.abort_transaction(txn)
                return False
        return False

    def _resolve_transfer(self, txn: Transaction) -> None:
        from_acc = txn.args["from_account"]
        to_acc = txn.args["to_account"]
        amount = txn.args["amount"]
        
        from_bal = self.execute_read(txn, from_acc)
        to_bal = self.execute_read(txn, to_acc)
        
        if from_bal is None or to_bal is None:
            raise RuntimeError("Account not found")
        if from_bal < amount:
            raise RuntimeError("Insufficient funds")
            
        self.execute_write(txn, from_acc, from_bal - amount)
        self.execute_write(txn, to_acc, to_bal + amount)

    def _resolve_withdraw(self, txn: Transaction) -> None:
        account_id = txn.args["account_id"]
        amount = txn.args["amount"]
        
        balance = self.execute_read(txn, account_id)
        if balance is None:
            raise RuntimeError("Account not found")
        if balance < amount:
            raise RuntimeError("Insufficient funds")
            
        self.execute_write(txn, account_id, balance - amount)

    def _resolve_deposit(self, txn: Transaction) -> None:
        account_id = txn.args["account_id"]
        amount = txn.args["amount"]
        
        balance = self.execute_read(txn, account_id)
        if balance is None:
            raise RuntimeError("Account not found")
            
        self.execute_write(txn, account_id, balance + amount)
    
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
        node_name = self.node_manager.get_node_for_account(account_id)
        if node_name is None:
            logger.error(f"[TxnManager] Txn {txn.txn_id}: Account {account_id} not found")
            return None
        
        # Acquire shared lock
        if not self.lock_manager.acquire_lock(txn.txn_id, node_name, account_id, LockType.SHARED):
            raise RuntimeError(f"Failed to acquire lock on account {account_id}")
        
        txn.add_lock(node_name, account_id, "S")
        
        # Read the balance using NodeManager
        return self.node_manager.read_balance(account_id)
    
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
            current_balance = self.node_manager.read_balance(account_id)
            if current_balance is not None:
                txn.record_read(node_name, account_id, current_balance)
        
        # Buffer the write
        # Buffer the write
        txn.buffer_write(node_name, account_id, balance)
        logger.debug(f"[TxnManager] Txn {txn.txn_id} buffered write: account {account_id} = {balance}")
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
        
        self.execute_write(txn, from_account, from_balance - amount)
        self.execute_write(txn, to_account, to_balance + amount)
        
        logger.info(f"[TxnManager] Txn {txn.txn_id} transfer: {amount} from {from_account} to {to_account}")
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
                self.node_manager.write_balance(op.account_id, op.value)
            
            # Mark transaction as committed
            txn.commit()
            
            # Release all locks (shrinking phase)
            self.lock_manager.release_all_locks(txn.txn_id)
            
            # Cleanup
            del self.active_transactions[txn.txn_id]
            
            print(f"[TxnManager] Transaction {txn.txn_id} committed successfully")
            return True
            
        except Exception as e:
            logger.error(f"[TxnManager] Commit failed for txn {txn.txn_id}: {e}")
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
        
        # Cleanup
        if txn.txn_id in self.active_transactions:
            del self.active_transactions[txn.txn_id]
        
        if txn.txn_id in self.active_transactions:
            del self.active_transactions[txn.txn_id]
        
        logger.info(f"[TxnManager] Transaction {txn.txn_id} aborted")
    
    def get_transaction(self, txn_id: int) -> Optional[Transaction]:
        """Get a transaction by ID"""
        return self.active_transactions.get(txn_id)
    
    def get_active_transaction_count(self) -> int:
        """Get the number of active transactions"""
        return len(self.active_transactions)
