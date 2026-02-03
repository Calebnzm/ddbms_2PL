"""
Lock Manager for Two-Phase Locking (2PL) Protocol

This module provides centralized lock management for distributed database nodes.
Implements Strict 2PL (SS2PL) semantics where locks are held until transaction commit/abort.
"""

import threading
from enum import Enum
from typing import Dict, Set, Optional, Tuple
from dataclasses import dataclass, field


class LockType(Enum):
    """Types of locks supported by the lock manager"""
    SHARED = "S"      # For read operations - multiple transactions can hold
    EXCLUSIVE = "X"   # For write operations - exclusive access required


@dataclass
class LockEntry:
    """Represents a lock on a specific resource (account)"""
    lock_type: LockType
    holders: Set[int] = field(default_factory=set)  # Transaction IDs holding this lock
    waiting: list = field(default_factory=list)     # (txn_id, lock_type) waiting for this lock


class LockManager:
    """
    Centralized lock manager for all database nodes.
    
    Manages lock acquisition, release, and upgrades for transactions
    following the Two-Phase Locking protocol.
    
    Thread-safe implementation using internal locking.
    """
    
    def __init__(self, lock_timeout: float = 10.0):
        """
        Initialize the lock manager.
        
        Args:
            lock_timeout: Maximum time (seconds) to wait for a lock before timing out
        """
        # locks: Dict[(node_name, account_id)] -> LockEntry
        self._locks: Dict[Tuple[str, int], LockEntry] = {}
        self._lock = threading.RLock()  # Internal lock for thread safety
        self._lock_timeout = lock_timeout
        # Condition variables for waiting transactions
        self._conditions: Dict[Tuple[str, int], threading.Condition] = {}
    
    def _get_resource_key(self, node_name: str, account_id: int) -> Tuple[str, int]:
        """Create a unique key for a resource"""
        return (node_name, account_id)
    
    def _get_condition(self, resource_key: Tuple[str, int]) -> threading.Condition:
        """Get or create a condition variable for a resource"""
        if resource_key not in self._conditions:
            self._conditions[resource_key] = threading.Condition(self._lock)
        return self._conditions[resource_key]
    
    def acquire_lock(
        self, 
        txn_id: int, 
        node_name: str, 
        account_id: int, 
        lock_type: LockType
    ) -> bool:
        """
        Acquire a lock on a resource.
        
        Args:
            txn_id: Transaction ID requesting the lock
            node_name: Name of the database node
            account_id: Account ID to lock
            lock_type: Type of lock (SHARED or EXCLUSIVE)
            
        Returns:
            True if lock acquired, False if timed out
        """
        resource_key = self._get_resource_key(node_name, account_id)
        condition = self._get_condition(resource_key)
        
        with condition:
            # Check if we already hold this lock
            if resource_key in self._locks:
                entry = self._locks[resource_key]
                if txn_id in entry.holders:
                    # Already hold a lock - check if upgrade needed
                    if entry.lock_type == LockType.SHARED and lock_type == LockType.EXCLUSIVE:
                        return self._upgrade_lock_internal(txn_id, resource_key, condition)
                    return True  # Already have sufficient lock
            
            # Try to acquire the lock
            return self._try_acquire_lock(txn_id, resource_key, lock_type, condition)
    
    def _try_acquire_lock(
        self, 
        txn_id: int, 
        resource_key: Tuple[str, int], 
        lock_type: LockType,
        condition: threading.Condition
    ) -> bool:
        """Internal method to try acquiring a lock with waiting"""
        start_waiting = False
        
        while True:
            if resource_key not in self._locks:
                # No lock exists - create new lock
                self._locks[resource_key] = LockEntry(
                    lock_type=lock_type,
                    holders={txn_id}
                )
                print(f"[LockManager] Txn {txn_id} acquired {lock_type.value} lock on {resource_key}")
                return True
            
            entry = self._locks[resource_key]
            
            # Check if lock can be granted
            if lock_type == LockType.SHARED:
                if entry.lock_type == LockType.SHARED:
                    # Multiple shared locks allowed
                    entry.holders.add(txn_id)
                    print(f"[LockManager] Txn {txn_id} acquired {lock_type.value} lock on {resource_key}")
                    return True
                # Exclusive lock held - must wait
            else:  # EXCLUSIVE
                if len(entry.holders) == 0:
                    # Can grant exclusive lock
                    entry.lock_type = LockType.EXCLUSIVE
                    entry.holders.add(txn_id)
                    print(f"[LockManager] Txn {txn_id} acquired {lock_type.value} lock on {resource_key}")
                    return True
                # Other transaction(s) hold lock - must wait
            
            # Add to waiting list if not already
            if not start_waiting:
                entry.waiting.append((txn_id, lock_type))
                start_waiting = True
                print(f"[LockManager] Txn {txn_id} waiting for {lock_type.value} lock on {resource_key}")
            
            # Wait for lock release with timeout
            notified = condition.wait(timeout=self._lock_timeout)
            if not notified:
                # Timeout - remove from waiting list and fail
                entry.waiting = [(t, lt) for t, lt in entry.waiting if t != txn_id]
                print(f"[LockManager] Txn {txn_id} timed out waiting for lock on {resource_key}")
                return False
    
    def _upgrade_lock_internal(
        self, 
        txn_id: int, 
        resource_key: Tuple[str, int],
        condition: threading.Condition
    ) -> bool:
        """Upgrade a shared lock to exclusive"""
        entry = self._locks[resource_key]
        
        while True:
            if len(entry.holders) == 1 and txn_id in entry.holders:
                # Only holder - can upgrade
                entry.lock_type = LockType.EXCLUSIVE
                print(f"[LockManager] Txn {txn_id} upgraded to X lock on {resource_key}")
                return True
            
            # Other holders exist - wait
            print(f"[LockManager] Txn {txn_id} waiting to upgrade lock on {resource_key}")
            notified = condition.wait(timeout=self._lock_timeout)
            if not notified:
                print(f"[LockManager] Txn {txn_id} timed out upgrading lock on {resource_key}")
                return False
    
    def upgrade_lock(self, txn_id: int, node_name: str, account_id: int) -> bool:
        """
        Upgrade a shared lock to exclusive lock.
        
        Args:
            txn_id: Transaction ID
            node_name: Name of the database node
            account_id: Account ID
            
        Returns:
            True if upgrade successful, False otherwise
        """
        resource_key = self._get_resource_key(node_name, account_id)
        condition = self._get_condition(resource_key)
        
        with condition:
            if resource_key not in self._locks:
                return False
            
            entry = self._locks[resource_key]
            if txn_id not in entry.holders:
                return False
            
            if entry.lock_type == LockType.EXCLUSIVE:
                return True  # Already exclusive
            
            return self._upgrade_lock_internal(txn_id, resource_key, condition)
    
    def release_lock(self, txn_id: int, node_name: str, account_id: int) -> None:
        """
        Release a lock held by a transaction.
        
        Args:
            txn_id: Transaction ID
            node_name: Name of the database node
            account_id: Account ID
        """
        resource_key = self._get_resource_key(node_name, account_id)
        condition = self._get_condition(resource_key)
        
        with condition:
            if resource_key not in self._locks:
                return
            
            entry = self._locks[resource_key]
            if txn_id in entry.holders:
                entry.holders.discard(txn_id)
                print(f"[LockManager] Txn {txn_id} released lock on {resource_key}")
                
                # If no holders left, clean up or grant to waiting transaction
                if len(entry.holders) == 0:
                    if entry.waiting:
                        # Wake up waiting transactions
                        condition.notify_all()
                    else:
                        # No waiters - remove lock entry
                        del self._locks[resource_key]
                else:
                    # Still have holders - notify for potential upgrades
                    condition.notify_all()
    
    def release_all_locks(self, txn_id: int) -> None:
        """
        Release all locks held by a transaction.
        
        This is called during transaction commit or abort.
        
        Args:
            txn_id: Transaction ID
        """
        with self._lock:
            # Find all resources locked by this transaction
            resources_to_release = []
            for resource_key, entry in self._locks.items():
                if txn_id in entry.holders:
                    resources_to_release.append(resource_key)
        
        # Release each lock
        for resource_key in resources_to_release:
            node_name, account_id = resource_key
            self.release_lock(txn_id, node_name, account_id)
        
        print(f"[LockManager] Txn {txn_id} released all locks ({len(resources_to_release)} total)")
    
    def get_lock_info(self, node_name: str, account_id: int) -> Optional[Dict]:
        """
        Get information about a lock (for debugging/monitoring).
        
        Returns:
            Dictionary with lock type and holders, or None if not locked
        """
        resource_key = self._get_resource_key(node_name, account_id)
        with self._lock:
            if resource_key not in self._locks:
                return None
            entry = self._locks[resource_key]
            return {
                "lock_type": entry.lock_type.value,
                "holders": list(entry.holders),
                "waiting": [(t, lt.value) for t, lt in entry.waiting]
            }
    
    def get_transaction_locks(self, txn_id: int) -> list:
        """
        Get all locks held by a transaction.
        
        Returns:
            List of (node_name, account_id, lock_type) tuples
        """
        with self._lock:
            result = []
            for resource_key, entry in self._locks.items():
                if txn_id in entry.holders:
                    node_name, account_id = resource_key
                    result.append((node_name, account_id, entry.lock_type.value))
            return result
