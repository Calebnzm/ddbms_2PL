import time
import threading
import os
import shutil
from node_manager import NodeManager
from transaction_manager import TransactionManager
from transaction import Transaction, TransactionType
from logger_config import setup_logger

logger = setup_logger(__name__)

def setup_demo_env():
    """Setup a clean database environment for the demo"""
    if os.path.exists("database_nodes"):
        shutil.rmtree("database_nodes")
    os.makedirs("database_nodes")
    
    nm = NodeManager("fragmentation.toml")
    # Reset CSV accounts if needed, but creating manually is safer for demo
    nm.create_account("Kisumu", 10000, account_id=1)
    nm.create_account("Nairobi", 5000, account_id=2)
    nm.create_account("Mombasa", 8000, account_id=3)
    nm.create_account("Kisumu", 3000, account_id=4)
    return nm

def run_scenario_1(tm):
    print("\n" + "="*60)
    print("SCENARIO 1: Successful Transfer (Happy Path)")
    print("Transferring 500 from Account 1 (Kisumu) -> Account 2 (Nairobi)")
    print("="*60)
    
    txn = Transaction(
        txn_type=TransactionType.TRANSFER,
        args={"from_account": 1, "to_account": 2, "amount": 500}
    )
    result = tm.execute_transaction(txn)
    print(f"Result: {'SUCCESS' if result else 'FAILURE'}")
    time.sleep(1)

def run_scenario_2(tm):
    print("\n" + "="*60)
    print("SCENARIO 2: Insufficient Funds (Business Logic Failure)")
    print("Attempting to withdraw 1,000,000 from Account 3 (Mombasa)")
    print("="*60)
    
    txn = Transaction(
        txn_type=TransactionType.WITHDRAW,
        args={"account_id": 3, "amount": 1000000}
    )
    result = tm.execute_transaction(txn)
    print(f"Result: {'SUCCESS' if result else 'FAILURE (Expected)'}")
    time.sleep(1)

def run_scenario_3(tm):
    print("\n" + "="*60)
    print("SCENARIO 3: Deadlock & Automatic Retry (Simulated)")
    print("Thread A: Acquire X(1), wait, Acquire X(2)")
    print("Thread B: Acquire X(2), wait, Acquire X(1)")
    print("This forces a textbook Wait-For Cycle: T_A -> T_B and T_B -> T_A")
    print("One transaction should detect the cycle, abort, and retry.")
    print("="*60)
    
    from lock_manager import LockType
    
    # We will use the TransactionManager to manage the lifecycle (begin/abort/commit)
    # but manually acquire locks to force the deadlock pattern.
    
    def txn_a_logic():
        t = Transaction(txn_type=TransactionType.TRANSFER, args={"from_account": 1, "to_account": 2, "amount": 10})
        # Use execute_transaction wrapper logic to handle retry? 
        # No, execute_transaction calls _resolve... which we want to avoid.
        # We must manually implement the retry loop here to show it off
        # OR better: monkeypatch _resolve_deposit for this instance? No.
        
        # We will implement a custom execution loop just for this demo
        max_retries = 3
        for attempt in range(max_retries):
            tm.active_transactions[t.txn_id] = t
            # Removed t.reset() from here
            logger.info(f"[Demo] Starting Thread A (Txn {t.txn_id}) - Attempt {attempt+1}")
            try:
                # 1. Acquire X Lock on 1
                if not tm.lock_manager.acquire_lock(t.txn_id, "Kisumu", 1, LockType.EXCLUSIVE):
                    raise RuntimeError("Failed to acquire lock 1")
                
                time.sleep(0.5) # Force wait for Thread B
                
                # 3. Acquire X Lock on 2
                if not tm.lock_manager.acquire_lock(t.txn_id, "Nairobi", 2, LockType.EXCLUSIVE):
                    raise RuntimeError("Failed to acquire lock 2")
                
                logger.info(f"[Demo] Txn {t.txn_id} finished successfully")
                tm.commit_transaction(t)
                return
                
            except Exception as e:
                logger.warning(f"[Demo] Txn {t.txn_id} failed: {e}")
                tm.abort_transaction(t)
                if "Deadlock" in str(e):
                    time.sleep(1) # Backoff
                    t.reset()
                    continue
                else:
                    return

    def txn_b_logic():
        t = Transaction(txn_type=TransactionType.TRANSFER, args={"from_account": 2, "to_account": 1, "amount": 10})
        max_retries = 3
        for attempt in range(max_retries):
            tm.active_transactions[t.txn_id] = t
            logger.info(f"[Demo] Starting Thread B (Txn {t.txn_id}) - Attempt {attempt+1}")
            try:
                # 1. Acquire X Lock on 2
                if not tm.lock_manager.acquire_lock(t.txn_id, "Nairobi", 2, LockType.EXCLUSIVE):
                    raise RuntimeError("Failed to acquire lock 2")
                
                time.sleep(0.5) # Force wait for Thread A
                
                # 2. Acquire X Lock on 1
                if not tm.lock_manager.acquire_lock(t.txn_id, "Kisumu", 1, LockType.EXCLUSIVE):
                     raise RuntimeError("Failed to acquire lock 1")
                
                logger.info(f"[Demo] Txn {t.txn_id} finished successfully")
                tm.commit_transaction(t)
                return
                
            except Exception as e:
                logger.warning(f"[Demo] Txn {t.txn_id} failed: {e}")
                tm.abort_transaction(t)
                if "Deadlock" in str(e):
                    time.sleep(1)
                    t.reset()
                    continue
                else:
                    return

    th1 = threading.Thread(target=txn_a_logic)
    th2 = threading.Thread(target=txn_b_logic)
    
    th1.start()
    th2.start()
    
    th1.join()
    th2.join()
    time.sleep(1)

if __name__ == "__main__":
    # Clear log file
    with open("system.log", "w") as f:
        f.truncate(0)
        
    print("Initializing System...")
    nm = setup_demo_env()
    # Increased timeout to allow deadlock detection to trigger reliably before timeout
    tm = TransactionManager(nm, lock_timeout=5.0)
    
    run_scenario_1(tm)
    run_scenario_2(tm)
    run_scenario_3(tm)
    
    print("\n" + "#"*60)
    print("DEMO COMPLETE. Checking Logs...")
    print("#"*60)
