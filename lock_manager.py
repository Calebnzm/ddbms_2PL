"""
Lock Manager for Two-Phase Locking (2PL) Protocol

This module provides centralized lock management for distributed database nodes.
Implements Strict 2PL (SS2PL) semantics where locks are held until transaction commit/abort.
"""

import threading
from enum import Enum
from typing import Dict, Set, Optional, Tuple, List
from dataclasses import dataclass, field
from logger_config import setup_logger

logger = setup_logger(__name__)


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


class DeadlockException(Exception):
    """Raised when a deadlock is detected"""
    pass


class WaitForGraph:
    """
    Directed graph representing transaction dependencies.
    Node A -> Node B means Transaction A is waiting for Transaction B.
    """
    def __init__(self):
        # adj[waiter] = {holders}
        self.adj: Dict[int, Set[int]] = {}
    
    def add_dependency(self, waiter: int, holder: int):
        """Record that waiter is waiting for holder"""
        if waiter not in self.adj:
            self.adj[waiter] = set()
        self.adj[waiter].add(holder)
        # Also ensure holder is in keys to simplify traversal logic if needed
        if holder not in self.adj:
            self.adj[holder] = set()
            
    def remove_transaction(self, txn_id: int):
        """Remove a transaction completely (e.g. on commit/abort)"""
        if txn_id in self.adj:
            del self.adj[txn_id]
        
        # Remove txn_id from others' dependency sets
        for waiter in self.adj:
            if txn_id in self.adj[waiter]:
                self.adj[waiter].discard(txn_id)

    def remove_waiting(self, txn_id: int):
        """Remove a transaction from waiting state (e.g. acquired lock)"""
        if txn_id in self.adj:
            self.adj[txn_id] = set() # Clear dependencies
            
    def detect_deadlock(self) -> Optional[int]:
        """
        Detect cycle in the graph.
        Returns: txn_id of a transaction in the cycle (victim), or None.
        """
        visited = set()
        stack = set()
        
        def visit(node):
            visited.add(node)
            stack.add(node)
            
            if node in self.adj:
                for neighbor in self.adj[node]:
                    if neighbor not in visited:
                        if visit(neighbor):
                            return True
                    elif neighbor in stack:
                        return True # Cycle found
            
            stack.remove(node)
            return False

        # Check all nodes
        nodes = list(self.adj.keys())
        for node in nodes:
            if node not in visited:
                if visit(node):
                    return node # Return the node that detected the cycle
        return None

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
        
        # Deadlock Detection
        self.wait_for_graph = WaitForGraph()
        
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
        """Internal method to try acquiring a lock with waiting (Deadlock Detection)"""
        start_waiting = False
        
        while True:
            if resource_key not in self._locks:
                # No lock exists - create new lock
                self._locks[resource_key] = LockEntry(
                    lock_type=lock_type,
                    holders={txn_id}
                )
                logger.info(f"[LockManager] Txn {txn_id} acquired {lock_type.value} lock on {resource_key}")
                # We acquired it, so we are not waiting anymore
                self.wait_for_graph.remove_waiting(txn_id)
                return True
            
            entry = self._locks[resource_key]
            
            # Check if lock can be granted
            can_grant = False
            if lock_type == LockType.SHARED:
                if entry.lock_type == LockType.SHARED:
                    can_grant = True
            else:  # EXCLUSIVE
                if len(entry.holders) == 0:
                    can_grant = True
            
            if can_grant:
                if lock_type == LockType.SHARED:
                   entry.holders.add(txn_id)
                else:
                   entry.lock_type = LockType.EXCLUSIVE
                   entry.holders.add(txn_id)
                
                logger.info(f"[LockManager] Txn {txn_id} acquired {lock_type.value} lock on {resource_key}")
                self.wait_for_graph.remove_waiting(txn_id)
                return True
            
            # BEFORE WAITING: Update Wait-For Graph and Detect Deadlock
            # Identify who we are waiting for
            conflicting_holders = list(entry.holders)
            
            # If we are already in the holders (e.g. upgrading), we don't wait for ourselves
            if txn_id in conflicting_holders:
                conflicting_holders.remove(txn_id)
            
            # Add edges
            for holder_id in conflicting_holders:
                self.wait_for_graph.add_dependency(txn_id, holder_id)
            
            # Check for deadlock
            victim = self.wait_for_graph.detect_deadlock()
            if victim is not None:
                # Cycle detected!
                logger.warning(f"[LockManager] DEADLOCK DETECTED involving {victim}!")
                # For simplicity, if a cycle is found when we try to wait, we abort OURSELVES (the requester).
                # This breaks the cycle immediately.
                # Remove our edges before raising
                self.wait_for_graph.remove_waiting(txn_id)
                raise DeadlockException(f"Deadlock detected involving transaction {txn_id}")

            # Add to waiting list if not already
            if not start_waiting:
                entry.waiting.append((txn_id, lock_type))
                start_waiting = True
                logger.debug(f"[LockManager] Txn {txn_id} waiting for {lock_type.value} lock on {resource_key}")
            
            # Wait for lock release with timeout
            notified = condition.wait(timeout=self._lock_timeout)
            if not notified:
                # Timeout - remove from waiting list and fail
                entry.waiting = [(t, lt) for t, lt in entry.waiting if t != txn_id]
                self.wait_for_graph.remove_waiting(txn_id)
                logger.warning(f"[LockManager] Txn {txn_id} timed out waiting for lock on {resource_key}")
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
                logger.info(f"[LockManager] Txn {txn_id} upgraded to X lock on {resource_key}")
                return True
            
            # Other holders exist - wait
            logger.debug(f"[LockManager] Txn {txn_id} waiting to upgrade lock on {resource_key}")
            notified = condition.wait(timeout=self._lock_timeout)
            if not notified:
                logger.warning(f"[LockManager] Txn {txn_id} timed out upgrading lock on {resource_key}")
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
                logger.debug(f"[LockManager] Txn {txn_id} released lock on {resource_key}")
                
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
        
        logger.info(f"[LockManager] Txn {txn_id} released all locks ({len(resources_to_release)} total)")
    
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
