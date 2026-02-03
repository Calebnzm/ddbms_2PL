#!/usr/bin/env python3
"""
Test Suite for Two-Phase Locking (2PL) Implementation

Tests cover:
1. Basic transaction read/write
2. Concurrent reads with shared locks
3. Read-write conflicts
4. Write-write conflicts
5. Transfer atomicity
6. Abort recovery
"""

import os
import sys
import threading
import time
from node_manager import NodeManager
from transaction_manager import TransactionManager


def setup_test_environment():
    """Set up a fresh test environment"""
    config_path = os.path.abspath("fragmentation.toml")
    
    # Remove existing databases for clean test
    for db_file in ["database_nodes/kisumu.db", "database_nodes/nairobi.db", "database_nodes/mombasa.db"]:
        if os.path.exists(db_file):
            os.remove(db_file)
    
    nm = NodeManager(config_path)
    
    # Create test accounts
    nm.create_account("Kisumu", initial_balance=10000)   # Account 1
    nm.create_account("Nairobi", initial_balance=5000)   # Account 2
    nm.create_account("Mombasa", initial_balance=8000)   # Account 3
    nm.create_account("Kisumu", initial_balance=3000)    # Account 4
    
    return nm


def test_basic_transaction():
    """Test 1: Basic transaction read and write"""
    print("\n" + "="*60)
    print("TEST 1: Basic Transaction Read/Write")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm)
    
    # Start a transaction
    txn = tm.begin_transaction()
    
    # Read initial balance
    balance = tm.execute_read(txn, 1)
    print(f"Initial balance of account 1: {balance}")
    assert balance == 10000, f"Expected 10000, got {balance}"
    
    # Write new balance
    tm.execute_write(txn, 1, 15000)
    
    # Commit
    tm.commit_transaction(txn)
    
    # Verify the write was applied
    nm2 = NodeManager(os.path.abspath("fragmentation.toml"))
    final_balance = nm2.read_balance(1)
    print(f"Final balance of account 1: {final_balance}")
    assert final_balance == 15000, f"Expected 15000, got {final_balance}"
    
    print("✓ TEST 1 PASSED: Basic transaction works correctly")
    return True


def test_concurrent_reads():
    """Test 2: Multiple transactions can hold shared locks simultaneously"""
    print("\n" + "="*60)
    print("TEST 2: Concurrent Reads (Shared Locks)")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm)
    
    results = {}
    
    def read_account(txn_name, account_id):
        txn = tm.begin_transaction()
        balance = tm.execute_read(txn, account_id)
        results[txn_name] = balance
        time.sleep(0.5)  # Hold the lock for a bit
        tm.commit_transaction(txn)
    
    # Start two concurrent reads on the same account
    t1 = threading.Thread(target=read_account, args=("T1", 1))
    t2 = threading.Thread(target=read_account, args=("T2", 1))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    print(f"T1 read: {results.get('T1')}")
    print(f"T2 read: {results.get('T2')}")
    
    assert results.get('T1') == 10000, f"T1 should read 10000"
    assert results.get('T2') == 10000, f"T2 should read 10000"
    
    print("✓ TEST 2 PASSED: Concurrent reads work correctly")
    return True


def test_read_write_conflict():
    """Test 3: Write must wait for read lock release"""
    print("\n" + "="*60)
    print("TEST 3: Read-Write Conflict")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm, lock_timeout=5.0)
    
    results = {"read_time": None, "write_time": None}
    
    def reader():
        txn = tm.begin_transaction()
        tm.execute_read(txn, 1)
        results["read_time"] = time.time()
        print(f"Reader acquired shared lock at {results['read_time']:.2f}")
        time.sleep(1.0)  # Hold the lock
        tm.commit_transaction(txn)
        print("Reader released lock")
    
    def writer():
        time.sleep(0.2)  # Start slightly after reader
        txn = tm.begin_transaction()
        print("Writer waiting for exclusive lock...")
        tm.execute_write(txn, 1, 20000)
        results["write_time"] = time.time()
        print(f"Writer acquired exclusive lock at {results['write_time']:.2f}")
        tm.commit_transaction(txn)
    
    t1 = threading.Thread(target=reader)
    t2 = threading.Thread(target=writer)
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    # Writer should have waited for reader
    time_diff = results["write_time"] - results["read_time"]
    print(f"Time between read lock and write lock: {time_diff:.2f}s")
    assert time_diff >= 0.8, "Writer should have waited for reader"
    
    print("✓ TEST 3 PASSED: Read-write conflict handled correctly")
    return True


def test_write_write_conflict():
    """Test 4: Exclusive lock prevents concurrent writes"""
    print("\n" + "="*60)
    print("TEST 4: Write-Write Conflict")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm, lock_timeout=5.0)
    
    order = []
    
    def writer(name, new_balance):
        txn = tm.begin_transaction()
        print(f"{name} attempting to acquire exclusive lock...")
        tm.execute_write(txn, 1, new_balance)
        order.append(f"{name}_acquired")
        print(f"{name} acquired exclusive lock")
        time.sleep(0.5)
        tm.commit_transaction(txn)
        order.append(f"{name}_released")
        print(f"{name} committed")
    
    t1 = threading.Thread(target=writer, args=("W1", 11111))
    t2 = threading.Thread(target=writer, args=("W2", 22222))
    
    t1.start()
    time.sleep(0.1)  # W1 gets lock first
    t2.start()
    
    t1.join()
    t2.join()
    
    print(f"Lock acquisition order: {order}")
    
    # W1 should acquire and release before W2 acquires
    assert order.index("W1_acquired") < order.index("W2_acquired"), \
        "W1 should acquire before W2"
    assert order.index("W1_released") < order.index("W2_acquired"), \
        "W1 should release before W2 acquires"
    
    # Final value should be from W2
    nm2 = NodeManager(os.path.abspath("fragmentation.toml"))
    final = nm2.read_balance(1)
    print(f"Final balance: {final}")
    assert final == 22222, f"Expected 22222 (W2's value), got {final}"
    
    print("✓ TEST 4 PASSED: Write-write conflict handled correctly")
    return True


def test_transfer_atomicity():
    """Test 5: Cross-node transfer succeeds or fails atomically"""
    print("\n" + "="*60)
    print("TEST 5: Transfer Atomicity")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm)
    
    # Get initial balances
    initial_1 = nm.read_balance(1)  # Kisumu - 10000
    initial_2 = nm.read_balance(2)  # Nairobi - 5000
    
    print(f"Initial: Account 1 = {initial_1}, Account 2 = {initial_2}")
    
    # Perform transfer
    txn = tm.begin_transaction()
    tm.transfer(txn, from_account=1, to_account=2, amount=3000)
    tm.commit_transaction(txn)
    
    # Verify final balances
    nm2 = NodeManager(os.path.abspath("fragmentation.toml"))
    final_1 = nm2.read_balance(1)
    final_2 = nm2.read_balance(2)
    
    print(f"Final: Account 1 = {final_1}, Account 2 = {final_2}")
    
    assert final_1 == 7000, f"Account 1 should be 7000, got {final_1}"
    assert final_2 == 8000, f"Account 2 should be 8000, got {final_2}"
    assert initial_1 + initial_2 == final_1 + final_2, "Total should be conserved"
    
    print("✓ TEST 5 PASSED: Transfer atomicity verified")
    return True


def test_abort_recovery():
    """Test 6: Aborted transaction releases all locks, no changes applied"""
    print("\n" + "="*60)
    print("TEST 6: Abort Recovery")
    print("="*60)
    
    nm = setup_test_environment()
    tm = TransactionManager(nm)
    
    initial = nm.read_balance(1)
    print(f"Initial balance: {initial}")
    
    # Start transaction, make writes, then abort
    txn = tm.begin_transaction()
    tm.execute_read(txn, 1)
    tm.execute_write(txn, 1, 99999)
    
    print("Aborting transaction...")
    tm.abort_transaction(txn)
    
    # Verify balance unchanged
    nm2 = NodeManager(os.path.abspath("fragmentation.toml"))
    final = nm2.read_balance(1)
    print(f"Final balance after abort: {final}")
    
    assert final == initial, f"Balance should be unchanged after abort"
    
    # Verify locks released by starting a new transaction
    txn2 = tm.begin_transaction()
    balance = tm.execute_read(txn2, 1)
    print(f"New transaction can read: {balance}")
    tm.commit_transaction(txn2)
    
    print("✓ TEST 6 PASSED: Abort recovery works correctly")
    return True


def run_all_tests():
    """Run all test cases"""
    print("\n" + "#"*60)
    print("# TWO-PHASE LOCKING (2PL) TEST SUITE")
    print("#"*60)
    
    tests = [
        ("Basic Transaction", test_basic_transaction),
        ("Concurrent Reads", test_concurrent_reads),
        ("Read-Write Conflict", test_read_write_conflict),
        ("Write-Write Conflict", test_write_write_conflict),
        ("Transfer Atomicity", test_transfer_atomicity),
        ("Abort Recovery", test_abort_recovery),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ TEST FAILED: {name}")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "="*60)
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)} tests")
    print("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
