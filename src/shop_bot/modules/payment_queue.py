"""
Payment Queue System

Provides robust payment processing with:
- Task persistence to prevent loss
- Retry mechanism with exponential backoff
- Duplicate prevention
- Fallback handling for API errors
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime, timedelta

from shop_bot.data_manager.database import (
    _exec, _fetch_row, _fetch_list, get_msk_time, DB_FILE
)

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class PaymentTask:
    task_id: str
    payment_id: str
    user_id: int
    action: str  # "top_up", "new", "extend"
    metadata: Dict[str, Any]
    status: str
    created_at: float
    updated_at: float
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    next_retry_at: Optional[float] = None


class PaymentQueue:
    """
    Persistent payment queue with retry logic and duplicate prevention.
    """

    def __init__(self):
        self._processing = False
        self._worker_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._handlers: Dict[str, Callable] = {}

    def initialize(self):
        """Initialize database tables for the queue"""
        self._ensure_tables()

    def _ensure_tables(self):
        """Create necessary tables if they don't exist"""
        import sqlite3
        try:
            with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS payment_queue (
                        task_id TEXT PRIMARY KEY,
                        payment_id TEXT UNIQUE NOT NULL,
                        user_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        metadata TEXT NOT NULL,
                        status TEXT DEFAULT 'pending',
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        error_message TEXT,
                        next_retry_at REAL
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_payment_queue_status ON payment_queue(status)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_payment_queue_payment_id ON payment_queue(payment_id)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_payment_queue_next_retry ON payment_queue(next_retry_at)
                ''')
                conn.commit()
                logger.info("Payment queue tables initialized")
        except Exception as e:
            logger.error(f"Failed to initialize payment queue tables: {e}")
            raise

    def register_handler(self, action: str, handler: Callable):
        """Register a handler for a specific action type"""
        self._handlers[action] = handler
        logger.debug(f"Registered handler for action: {action}")

    async def add_task(self, payment_id: str, user_id: int, action: str,
                       metadata: Dict[str, Any], max_retries: int = 3) -> PaymentTask:
        """
        Add a new payment task to the queue.
        Returns existing task if payment_id already exists (duplicate prevention).
        """
        # Check for existing task with same payment_id
        existing = self._get_task_by_payment_id(payment_id)
        if existing:
            logger.warning(f"Duplicate payment attempt blocked: {payment_id}")
            return existing

        task_id = f"task_{payment_id}_{int(time.time())}"
        now = time.time()

        task = PaymentTask(
            task_id=task_id,
            payment_id=payment_id,
            user_id=user_id,
            action=action,
            metadata=metadata,
            status=TaskStatus.PENDING.value,
            created_at=now,
            updated_at=now,
            max_retries=max_retries
        )

        self._save_task(task)
        logger.info(f"Added payment task {task_id} for payment {payment_id}")
        return task

    def _get_task_by_payment_id(self, payment_id: str) -> Optional[PaymentTask]:
        """Get task by payment_id (for duplicate check)"""
        row = _fetch_row(
            "SELECT * FROM payment_queue WHERE payment_id = ? LIMIT 1",
            (payment_id,),
            "Failed to check existing payment task"
        )
        if row:
            return self._row_to_task(dict(row))
        return None

    def _get_task(self, task_id: str) -> Optional[PaymentTask]:
        """Get task by task_id"""
        row = _fetch_row(
            "SELECT * FROM payment_queue WHERE task_id = ? LIMIT 1",
            (task_id,),
            "Failed to get payment task"
        )
        if row:
            return self._row_to_task(dict(row))
        return None

    def _save_task(self, task: PaymentTask):
        """Save task to database"""
        _exec('''
            INSERT OR REPLACE INTO payment_queue
            (task_id, payment_id, user_id, action, metadata, status, created_at, updated_at,
             retry_count, max_retries, error_message, next_retry_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task.task_id, task.payment_id, task.user_id, task.action,
            json.dumps(task.metadata, ensure_ascii=False),
            task.status, task.created_at, task.updated_at,
            task.retry_count, task.max_retries, task.error_message, task.next_retry_at
        ), f"Failed to save payment task {task.task_id}")

    def _row_to_task(self, row: Dict) -> PaymentTask:
        """Convert database row to PaymentTask"""
        return PaymentTask(
            task_id=row['task_id'],
            payment_id=row['payment_id'],
            user_id=row['user_id'],
            action=row['action'],
            metadata=json.loads(row['metadata']) if row['metadata'] else {},
            status=row['status'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
            error_message=row['error_message'],
            next_retry_at=row['next_retry_at']
        )

    async def start_worker(self):
        """Start the background worker to process tasks"""
        if self._processing:
            logger.debug("Payment queue worker already running")
            return

        self._processing = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("Payment queue worker started")

    async def stop_worker(self):
        """Stop the background worker"""
        self._processing = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("Payment queue worker stopped")

    async def _worker_loop(self):
        """Main worker loop - processes pending tasks"""
        while self._processing:
            try:
                await self._process_pending_tasks()
            except Exception as e:
                logger.error(f"Error in payment queue worker: {e}")
            await asyncio.sleep(5)  # Check every 5 seconds

    async def _process_pending_tasks(self):
        """Process all pending or retry-ready tasks"""
        now = time.time()

        # Get tasks that are pending or ready for retry
        rows = _fetch_list('''
            SELECT * FROM payment_queue
            WHERE status IN ('pending', 'retrying')
               OR (status = 'failed' AND next_retry_at <= ? AND retry_count < max_retries)
            ORDER BY created_at ASC
            LIMIT 10
        ''', (now,), "Failed to fetch pending tasks")

        for row in rows:
            task = self._row_to_task(dict(row))
            await self._process_task(task)

    async def _process_task(self, task: PaymentTask):
        """Process a single task with retry logic"""
        async with self._lock:
            # Refresh task status
            fresh_task = self._get_task(task.task_id)
            if not fresh_task or fresh_task.status == TaskStatus.COMPLETED.value:
                return

            # Mark as processing
            task.status = TaskStatus.PROCESSING.value
            task.updated_at = time.time()
            self._save_task(task)

        handler = self._handlers.get(task.action)
        if not handler:
            logger.error(f"No handler registered for action: {task.action}")
            task.status = TaskStatus.FAILED.value
            task.error_message = f"No handler for action: {task.action}"
            self._save_task(task)
            return

        try:
            logger.info(f"Processing payment task {task.task_id} (attempt {task.retry_count + 1})")
            result = await handler(task.metadata)

            if result:
                # Success
                task.status = TaskStatus.COMPLETED.value
                task.error_message = None
                logger.info(f"Payment task {task.task_id} completed successfully")
            else:
                # Handler returned False - treat as failure but retryable
                raise RuntimeError("Payment handler returned False")

        except Exception as e:
            logger.error(f"Payment task {task.task_id} failed: {e}")
            task.retry_count += 1

            if task.retry_count >= task.max_retries:
                task.status = TaskStatus.FAILED.value
                task.error_message = str(e)[:500]
                logger.error(f"Payment task {task.task_id} exhausted all retries")
            else:
                # Schedule retry with exponential backoff
                backoff = min(2 ** task.retry_count, 300)  # Max 5 minutes
                task.status = TaskStatus.RETRYING.value
                task.next_retry_at = time.time() + backoff
                task.error_message = str(e)[:500]
                logger.info(f"Payment task {task.task_id} scheduled for retry in {backoff}s")

        task.updated_at = time.time()
        self._save_task(task)

    def get_task_status(self, payment_id: str) -> Optional[Dict]:
        """Get status of a task by payment_id"""
        task = self._get_task_by_payment_id(payment_id)
        if task:
            return {
                'task_id': task.task_id,
                'status': task.status,
                'retry_count': task.retry_count,
                'max_retries': task.max_retries,
                'error_message': task.error_message,
                'created_at': task.created_at,
                'updated_at': task.updated_at
            }
        return None

    def cleanup_old_tasks(self, days: int = 7):
        """Remove completed/failed tasks older than specified days"""
        cutoff = time.time() - (days * 24 * 3600)
        cursor = _exec(
            "DELETE FROM payment_queue WHERE updated_at < ? AND status IN ('completed', 'failed')",
            (cutoff,),
            "Failed to cleanup old payment tasks"
        )
        if cursor:
            logger.info(f"Cleaned up {cursor.rowcount} old payment tasks")


# Global queue instance
_queue_instance: Optional[PaymentQueue] = None


def get_payment_queue() -> PaymentQueue:
    """Get the global payment queue instance"""
    global _queue_instance
    if _queue_instance is None:
        _queue_instance = PaymentQueue()
    return _queue_instance


async def queue_payment(payment_id: str, user_id: int, action: str,
                        metadata: Dict[str, Any], max_retries: int = 3) -> PaymentTask:
    """
    Convenience function to add a payment to the queue.
    """
    queue = get_payment_queue()
    return await queue.add_task(payment_id, user_id, action, metadata, max_retries)


def get_payment_status(payment_id: str) -> Optional[Dict]:
    """Get status of a queued payment"""
    queue = get_payment_queue()
    return queue.get_task_status(payment_id)
