"""
Transaction Module for Two-Phase Locking (2PL) Protocol

This module provides transaction management with 2PL semantics.
Transactions follow the growing and shrinking phases for lock acquisition/release.
"""

from enum import Enum
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass, field
import threading


class TransactionType(Enum):
    """High-level transaction types"""
    TRANSFER = "transfer"
    WITHDRAW = "withdraw"
    DEPOSIT = "deposit"

class OperationType(Enum):
    """Types of operations in a transaction"""
    READ = "read"
    WRITE = "write"

class TransactionState(Enum):
    """Possible states of a transaction"""
    ACTIVE = "active"         # Transaction is running
    COMMITTED = "committed"   # Transaction successfully committed
    ABORTED = "aborted"       # Transaction was aborted/rolled back


class TransactionPhase(Enum):
    """Two-Phase Locking phases"""
    GROWING = "growing"       # Can acquire locks, cannot release
    SHRINKING = "shrinking"   # Can release locks, cannot acquire


@dataclass
class Operation:
    """Represents a transaction operation"""
    op_type: OperationType
    account_id: int
    value: Any = None                # New balance value (for write)
    node_name: str = ""              # Resolved at runtime


@dataclass 
class LockHeld:
    """Represents a lock held by a transaction"""
    node_name: str
    account_id: int
    lock_type: str            # "S" or "X"


class Transaction:
    """
    Represents a database transaction with Two-Phase Locking semantics.
    
    Implements Strict 2PL (SS2PL) where all locks are held until commit/abort.
    
    Attributes:
        txn_id: Unique transaction identifier
        state: Current transaction state (ACTIVE, COMMITTED, ABORTED)
        phase: Current 2PL phase (GROWING or SHRINKING)
        held_locks: List of locks currently held
        write_buffer: Buffered write operations (applied at commit)
        read_set: Accounts that have been read
    """
    
    _next_txn_id = 1
    _id_lock = None
    
    @classmethod
    def _get_next_id(cls) -> int:
        """Thread-safe transaction ID generation"""
        if cls._id_lock is None:
            cls._id_lock = threading.Lock()
        with cls._id_lock:
            txn_id = cls._next_txn_id
            cls._next_txn_id += 1
            return txn_id
    
    def __init__(self, txn_type: TransactionType = None, args: dict = None, txn_id: Optional[int] = None):
        """
        Initialize a new transaction.
        
        Args:
            txn_type: High-level transaction type (TRANSFER, WITHDRAW, DEPOSIT)
            args: Dictionary of arguments for the transaction type
            txn_id: Optional transaction ID. If not provided, auto-generates one.
        """
        self.txn_id = txn_id if txn_id is not None else Transaction._get_next_id()
        self.state = TransactionState.ACTIVE
        self.phase = TransactionPhase.GROWING
        self.txn_type = txn_type
        self.args = args if args else {}
        self.held_locks: List[LockHeld] = []
        self.write_buffer: List[Operation] = []
        self.read_set: List[Tuple[str, int]] = []  # (node_name, account_id)
        self._original_values: dict = {}  # For rollback: account_id -> original_value
        
        print(f"[Transaction] Txn {self.txn_id} started")
    
    def add_lock(self, node_name: str, account_id: int, lock_type: str) -> None:
        """
        Record a lock acquisition.
        
        Args:
            node_name: Name of the database node
            account_id: Account ID that was locked
            lock_type: Type of lock ("S" or "X")
        """
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"Cannot acquire lock: Transaction {self.txn_id} is {self.state.value}")
        
        if self.phase != TransactionPhase.GROWING:
            raise RuntimeError(f"Cannot acquire lock: Transaction {self.txn_id} is in {self.phase.value} phase")
        
        # Check if we already have this lock
        for lock in self.held_locks:
            if lock.node_name == node_name and lock.account_id == account_id:
                # Update lock type if upgrading
                if lock.lock_type == "S" and lock_type == "X":
                    lock.lock_type = "X"
                return
        
        self.held_locks.append(LockHeld(node_name, account_id, lock_type))
    
    def record_read(self, node_name: str, account_id: int, value: Any) -> None:
        """
        Record a read operation.
        
        Args:
            node_name: Name of the database node
            account_id: Account ID that was read
            value: Value that was read
        """
        self.read_set.append((node_name, account_id))
        # Store original value for potential rollback
        if account_id not in self._original_values:
            self._original_values[account_id] = value
    
    def buffer_write(self, node_name: str, account_id: int, value: Any) -> None:
        """
        Buffer a write operation (deferred until commit).
        
        Args:
            node_name: Name of the database node
            account_id: Account ID to write to
            value: New balance value
        """
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"Cannot write: Transaction {self.txn_id} is {self.state.value}")
        
        self.write_buffer.append(Operation(
            op_type=OperationType.WRITE,
            account_id=account_id,
            value=value,
            node_name=node_name
        ))
        print(f"[Transaction] Txn {self.txn_id} buffered write: account {account_id} = {value}")
    
    def get_write_buffer(self) -> List[Operation]:
        """Get the list of buffered write operations"""
        return self.write_buffer.copy()
    
    def get_held_locks(self) -> List[LockHeld]:
        """Get the list of held locks"""
        return self.held_locks.copy()
    
    def get_original_value(self, account_id: int) -> Optional[Any]:
        """Get the original value of an account (for rollback)"""
        return self._original_values.get(account_id)
    
    def enter_shrinking_phase(self) -> None:
        """
        Enter the shrinking phase of 2PL.
        
        In Strict 2PL, this happens at commit/abort time.
        After this, no new locks can be acquired.
        """
        if self.phase == TransactionPhase.GROWING:
            self.phase = TransactionPhase.SHRINKING
            print(f"[Transaction] Txn {self.txn_id} entered shrinking phase")
    
    def commit(self) -> None:
        """
        Mark the transaction as committed.
        
        Note: Actual lock release is handled by TransactionManager.
        """
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"Cannot commit: Transaction {self.txn_id} is already {self.state.value}")
        
        self.enter_shrinking_phase()
        self.state = TransactionState.COMMITTED
        print(f"[Transaction] Txn {self.txn_id} committed ({len(self.write_buffer)} writes, {len(self.held_locks)} locks)")
    
    def abort(self) -> None:
        """
        Mark the transaction as aborted.
        
        Note: Actual rollback and lock release is handled by TransactionManager.
        """
        if self.state == TransactionState.COMMITTED:
            raise RuntimeError(f"Cannot abort: Transaction {self.txn_id} is already committed")
        
        self.enter_shrinking_phase()
        self.state = TransactionState.ABORTED
        print(f"[Transaction] Txn {self.txn_id} aborted")
    
    def is_active(self) -> bool:
        """Check if transaction is still active"""
        return self.state == TransactionState.ACTIVE
    
    def is_committed(self) -> bool:
        """Check if transaction was committed"""
        return self.state == TransactionState.COMMITTED
    
    def is_aborted(self) -> bool:
        """Check if transaction was aborted"""
        return self.state == TransactionState.ABORTED
    
    def __repr__(self) -> str:
        return (f"Transaction(id={self.txn_id}, state={self.state.value}, "
                f"phase={self.phase.value}, locks={len(self.held_locks)}, "
                f"buffered_writes={len(self.write_buffer)})")
