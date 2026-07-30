import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from shop_bot.data_manager import database


class TelegramStarChargeRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.original_db_file = database.DB_FILE
        database.DB_FILE = Path(self.tempdir.name) / "users.db"
        database.initialize_db()

    def tearDown(self):
        database.DB_FILE = self.original_db_file
        self.tempdir.cleanup()

    def test_registry_cutoff_is_initialized_once(self):
        with sqlite3.connect(database.DB_FILE) as conn:
            first = conn.execute(
                "SELECT value FROM bot_settings WHERE key = ?",
                ("telegram_star_charge_registry_started_at",),
            ).fetchone()[0]
            conn.execute(
                "UPDATE bot_settings SET value = ? WHERE key = ?",
                ("2000-01-01T00:00:00+00:00", "telegram_star_charge_registry_started_at"),
            )
            conn.commit()

        self.assertTrue(first)
        database.initialize_db()
        with sqlite3.connect(database.DB_FILE) as conn:
            preserved = conn.execute(
                "SELECT value FROM bot_settings WHERE key = ?",
                ("telegram_star_charge_registry_started_at",),
            ).fetchone()[0]
        self.assertEqual(preserved, "2000-01-01T00:00:00+00:00")

    def test_charge_is_claimed_once_and_keeps_terminal_status(self):
        first = database.claim_telegram_star_charge(
            "charge-1",
            "invoice-1",
            123,
            90,
        )
        second = database.claim_telegram_star_charge(
            "charge-1",
            "invoice-1",
            123,
            90,
        )
        self.assertEqual(first, "claimed")
        self.assertEqual(second, "processing")

        self.assertTrue(
            database.set_telegram_star_charge_status("charge-1", "processed")
        )
        third = database.claim_telegram_star_charge(
            "charge-1",
            "invoice-1",
            123,
            90,
        )
        self.assertEqual(third, "processed")

        with sqlite3.connect(database.DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT invoice_payload, user_id, stars_amount, status
                FROM telegram_star_charges
                WHERE charge_id = ?
                """,
                ("charge-1",),
            ).fetchone()
        self.assertEqual(row, ("invoice-1", 123, 90, "processed"))

    def test_invalid_terminal_status_is_rejected(self):
        database.claim_telegram_star_charge("charge-2", "invoice-2", 123, 90)
        with self.assertRaises(ValueError):
            database.set_telegram_star_charge_status("charge-2", "processing")

    def test_cancelled_pending_row_cannot_be_completed(self):
        metadata = {
            "user_id": 123,
            "price": 150.0,
            "payment_method": "Telegram Stars",
        }
        with sqlite3.connect(database.DB_FILE) as conn:
            conn.execute(
                """
                INSERT INTO pending_transactions
                    (payment_id, user_id, amount_rub, metadata, status)
                VALUES (?, ?, ?, ?, 'cancelled')
                """,
                ("invoice-cancelled", 123, 150.0, json.dumps(metadata)),
            )

        snapshot = database.get_pending_transaction_snapshot("invoice-cancelled")
        self.assertEqual(snapshot["status"], "cancelled")
        self.assertEqual(snapshot["metadata"]["user_id"], 123)
        self.assertIsNone(
            database.find_and_complete_pending_transaction("invoice-cancelled")
        )
        with sqlite3.connect(database.DB_FILE) as conn:
            status = conn.execute(
                "SELECT status FROM pending_transactions WHERE payment_id = ?",
                ("invoice-cancelled",),
            ).fetchone()[0]
        self.assertEqual(status, "cancelled")

    def test_snapshot_does_not_hide_invalid_metadata(self):
        with sqlite3.connect(database.DB_FILE) as conn:
            conn.execute(
                """
                INSERT INTO pending_transactions
                    (payment_id, user_id, amount_rub, metadata, status)
                VALUES (?, ?, ?, ?, 'pending')
                """,
                ("invoice-broken", 123, 150.0, "{broken-json"),
            )
        with self.assertRaises(json.JSONDecodeError):
            database.get_pending_transaction_snapshot("invoice-broken")


if __name__ == "__main__":
    unittest.main()
