import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from shop_bot.bot import handlers
from shop_bot.data_manager import database


BASE_METADATA = {
    "user_id": 123,
    "price": 150.0,
    "action": "extend",
    "payment_method": "Telegram Stars",
    "months": 1,
    "key_id": 14,
    "host_name": "ProstoVPN",
    "plan_id": 3,
}


def make_message(
    *,
    payload="invoice-1",
    charge_id="charge-1",
    stars=90,
    user_id=123,
    date=datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc),
):
    return SimpleNamespace(
        successful_payment=SimpleNamespace(
            invoice_payload=payload,
            telegram_payment_charge_id=charge_id,
            total_amount=stars,
        ),
        from_user=SimpleNamespace(id=user_id, username="tester"),
        date=date,
    )


class StarsPaymentHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handlers._STARS_USER_LOCKS.clear()

    async def invoke(
        self,
        *,
        status,
        metadata=None,
        duplicate=False,
        claim_status="claimed",
        processed=True,
        transaction_recorded=True,
        complete_pending=True,
        refreshed_status="paid",
        snapshot_error=None,
        message=None,
    ):
        bot = SimpleNamespace(
            refund_star_payment=AsyncMock(return_value=True),
            send_message=AsyncMock(return_value=True),
        )
        process = AsyncMock(return_value=processed)
        pending_metadata = dict(metadata) if metadata is not None else None
        complete_result = dict(metadata) if metadata is not None and complete_pending else None
        initial_snapshot = (
            {"status": status, "metadata": pending_metadata}
            if status is not None or pending_metadata is not None
            else None
        )
        if snapshot_error is not None:
            snapshot_lookup = Mock(side_effect=snapshot_error)
        elif status == "pending" and not complete_pending:
            snapshot_lookup = Mock(
                side_effect=[
                    initial_snapshot,
                    {"status": refreshed_status, "metadata": pending_metadata},
                ]
            )
        else:
            snapshot_lookup = Mock(return_value=initial_snapshot)
        transaction_checks = Mock(
            side_effect=[True] if duplicate else [False, transaction_recorded]
        )
        self.claim_charge = Mock(return_value=claim_status)
        self.set_charge_status = Mock(return_value=True)

        with (
            patch.object(handlers, "check_transaction_exists", transaction_checks),
            patch.object(handlers, "claim_telegram_star_charge", self.claim_charge),
            patch.object(handlers, "set_telegram_star_charge_status", self.set_charge_status),
            patch.object(handlers, "get_pending_transaction_snapshot", snapshot_lookup),
            patch.object(handlers, "find_and_complete_pending_transaction", Mock(return_value=complete_result)),
            patch.object(
                handlers,
                "get_setting",
                Mock(side_effect=lambda key: "2026-07-30T10:00:00+00:00" if key == "telegram_star_charge_registry_started_at" else "0.6"),
            ),
            patch.object(handlers, "process_successful_payment", process),
        ):
            result = await handlers._handle_stars_success_payment(message or make_message(), bot)
        return result, bot, process

    async def test_create_pending_payment_persists_stars_metadata(self):
        original_db_file = database.DB_FILE
        with tempfile.TemporaryDirectory() as tempdir:
            database.DB_FILE = Path(tempdir) / "users.db"
            database.initialize_db()
            try:
                payment_id, metadata = await handlers.create_pending_payment(
                    user_id=123,
                    amount=100.0,
                    payment_method="Telegram Stars",
                    action="new",
                    metadata_source={"stars_amount": 60, "currency": "XTR"},
                    plan_id=1,
                    months=1,
                )
                snapshot = database.get_pending_transaction_snapshot(payment_id)
            finally:
                database.DB_FILE = original_db_file

        self.assertEqual(metadata["stars_amount"], 60)
        self.assertEqual(metadata["currency"], "XTR")
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["metadata"]["stars_amount"], 60)
        self.assertEqual(snapshot["metadata"]["currency"], "XTR")

    async def test_pending_invoice_uses_unique_charge_id(self):
        result, bot, process = await self.invoke(status="pending", metadata=BASE_METADATA)
        self.assertTrue(result)
        bot.refund_star_payment.assert_not_awaited()
        processed = process.await_args.args[1]
        self.assertEqual(processed["payment_id"], "charge-1")
        self.assertEqual(processed["invoice_payload"], "invoice-1")
        self.assertEqual(processed["telegram_payment_charge_id"], "charge-1")
        self.assertEqual(processed["stars_amount"], 90)
        self.claim_charge.assert_called_once_with("charge-1", "invoice-1", 123, 90)
        self.set_charge_status.assert_called_once_with("charge-1", "processed")

    async def test_historical_successful_payment_is_held_for_manual_review(self):
        message = make_message(date=datetime(2026, 7, 30, 9, 0, tzinfo=timezone.utc))
        result, bot, process = await self.invoke(status="paid", metadata=BASE_METADATA, message=message)
        self.assertFalse(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        self.set_charge_status.assert_called_once_with(
            "charge-1",
            "failed",
            "successful_payment создан до инициализации charge registry",
        )

    async def test_paid_invoice_can_be_paid_again_with_new_charge(self):
        result, bot, process = await self.invoke(status="paid", metadata=BASE_METADATA)
        self.assertTrue(result)
        bot.refund_star_payment.assert_not_awaited()
        processed = process.await_args.args[1]
        self.assertEqual(processed["action"], "extend")
        self.assertEqual(processed["payment_id"], "charge-1")
        self.assertEqual(processed["pending_status_before_payment"], "paid")

    async def test_duplicate_webhook_is_idempotent(self):
        result, bot, process = await self.invoke(status="paid", metadata=BASE_METADATA, duplicate=True)
        self.assertTrue(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        self.claim_charge.assert_not_called()

    async def test_claimed_charge_is_not_processed_twice(self):
        result, bot, process = await self.invoke(
            status="paid",
            metadata=BASE_METADATA,
            claim_status="processing",
        )
        self.assertTrue(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        self.set_charge_status.assert_not_called()

    async def test_uncertain_claim_is_not_refunded_or_processed(self):
        result, bot, process = await self.invoke(
            status="paid",
            metadata=BASE_METADATA,
            claim_status="error",
        )
        self.assertFalse(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        bot.send_message.assert_awaited_once()
        self.set_charge_status.assert_not_called()

    async def test_snapshot_error_is_held_for_manual_reconciliation(self):
        result, bot, process = await self.invoke(
            status="paid",
            metadata=BASE_METADATA,
            snapshot_error=RuntimeError("database locked"),
        )
        self.assertFalse(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        self.set_charge_status.assert_called_once_with(
            "charge-1",
            "failed",
            "ошибка чтения invoice: RuntimeError",
        )

    async def test_pending_compare_and_set_race_reloads_paid_invoice(self):
        result, bot, process = await self.invoke(
            status="pending",
            metadata=BASE_METADATA,
            complete_pending=False,
            refreshed_status="paid",
        )
        self.assertTrue(result)
        bot.refund_star_payment.assert_not_awaited()
        process.assert_awaited_once()
        processed = process.await_args.args[1]
        self.assertEqual(processed["action"], "extend")
        self.assertEqual(processed["pending_status_before_payment"], "paid")

    async def test_pending_compare_and_set_failure_never_becomes_topup(self):
        result, bot, process = await self.invoke(
            status="pending",
            metadata=BASE_METADATA,
            complete_pending=False,
            refreshed_status="pending",
        )
        self.assertFalse(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_not_awaited()
        self.set_charge_status.assert_called_once_with(
            "charge-1",
            "failed",
            "pending invoice не удалось атомарно завершить",
        )

    async def test_failed_processing_is_left_for_manual_reconciliation(self):
        result, bot, process = await self.invoke(
            status="paid",
            metadata=BASE_METADATA,
            processed=False,
        )
        self.assertFalse(result)
        process.assert_awaited_once()
        self.set_charge_status.assert_called_once_with(
            "charge-1",
            "failed",
            "обработчик вернул ошибку",
        )

    async def test_missing_transaction_after_processing_is_failed(self):
        result, bot, process = await self.invoke(
            status="paid",
            metadata=BASE_METADATA,
            transaction_recorded=False,
        )
        self.assertFalse(result)
        process.assert_awaited_once()
        self.set_charge_status.assert_called_once_with(
            "charge-1",
            "failed",
            "операция выполнена без записи transactions",
        )

    async def test_two_unique_charges_for_one_user_are_serialized(self):
        recorded = set()
        active = 0
        max_active = 0

        def transaction_exists(charge_id):
            return charge_id in recorded

        async def process(_bot, metadata):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            recorded.add(metadata["payment_id"])
            active -= 1
            return True

        bot = SimpleNamespace(
            refund_star_payment=AsyncMock(return_value=True),
            send_message=AsyncMock(return_value=True),
        )
        snapshot = {"status": "paid", "metadata": dict(BASE_METADATA)}
        with (
            patch.object(handlers, "check_transaction_exists", Mock(side_effect=transaction_exists)),
            patch.object(handlers, "claim_telegram_star_charge", Mock(return_value="claimed")),
            patch.object(handlers, "set_telegram_star_charge_status", Mock(return_value=True)),
            patch.object(handlers, "get_pending_transaction_snapshot", Mock(return_value=snapshot)),
            patch.object(handlers, "get_setting", Mock(return_value="2026-07-30T10:00:00+00:00")),
            patch.object(handlers, "process_successful_payment", process),
        ):
            results = await asyncio.gather(
                handlers._handle_stars_success_payment(make_message(charge_id="charge-a"), bot),
                handlers._handle_stars_success_payment(make_message(charge_id="charge-b"), bot),
            )

        self.assertEqual(results, [True, True])
        self.assertEqual(recorded, {"charge-a", "charge-b"})
        self.assertEqual(max_active, 1)
        bot.refund_star_payment.assert_not_awaited()

    async def test_cancelled_invoice_is_refunded(self):
        result, bot, process = await self.invoke(status="cancelled", metadata=BASE_METADATA)
        self.assertTrue(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_awaited_once_with(
            user_id=123,
            telegram_payment_charge_id="charge-1",
        )
        self.set_charge_status.assert_called_once_with("charge-1", "refunded", None)

    async def test_unknown_invoice_becomes_balance_topup(self):
        result, bot, process = await self.invoke(status=None, metadata=None)
        self.assertTrue(result)
        bot.refund_star_payment.assert_not_awaited()
        processed = process.await_args.args[1]
        self.assertEqual(processed["action"], "top_up")
        self.assertEqual(processed["price"], 150.0)
        self.assertEqual(processed["payment_id"], "charge-1")

    async def test_invoice_for_another_user_is_refunded(self):
        metadata = dict(BASE_METADATA, user_id=999)
        result, bot, process = await self.invoke(status="paid", metadata=metadata)
        self.assertTrue(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_awaited_once()

    async def test_stars_amount_mismatch_is_refunded(self):
        metadata = dict(BASE_METADATA, stars_amount=100)
        result, bot, process = await self.invoke(status="paid", metadata=metadata)
        self.assertTrue(result)
        process.assert_not_awaited()
        bot.refund_star_payment.assert_awaited_once()


class StarsPreCheckoutTests(unittest.IsolatedAsyncioTestCase):
    async def invoke(self, *, status, metadata, stars=90, user_id=123, snapshot_error=None):
        query = SimpleNamespace(
            invoice_payload="invoice-1",
            total_amount=stars,
            from_user=SimpleNamespace(id=user_id),
            answer=AsyncMock(return_value=True),
        )
        snapshot = {"status": status, "metadata": metadata} if status is not None or metadata is not None else None
        lookup = Mock(side_effect=snapshot_error) if snapshot_error is not None else Mock(return_value=snapshot)
        with patch.object(handlers, "get_pending_transaction_snapshot", lookup):
            result = await handlers._handle_stars_pre_checkout(query)
        return result, query

    async def test_cancelled_invoice_is_rejected_before_charge(self):
        result, query = await self.invoke(status="cancelled", metadata=BASE_METADATA)
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_paid_legacy_invoice_is_allowed_for_same_user(self):
        result, query = await self.invoke(status="paid", metadata=BASE_METADATA)
        self.assertTrue(result)
        query.answer.assert_awaited_once_with(ok=True)

    async def test_invoice_for_another_user_is_rejected(self):
        result, query = await self.invoke(status="pending", metadata=dict(BASE_METADATA, user_id=999))
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_unknown_invoice_is_rejected(self):
        result, query = await self.invoke(status=None, metadata=None)
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_missing_status_is_rejected(self):
        result, query = await self.invoke(status=None, metadata=BASE_METADATA)
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_invalid_status_is_rejected(self):
        result, query = await self.invoke(status="failed", metadata=BASE_METADATA)
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_snapshot_error_is_rejected(self):
        result, query = await self.invoke(status="pending", metadata=BASE_METADATA, snapshot_error=RuntimeError("locked"))
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])

    async def test_stars_amount_mismatch_is_rejected(self):
        result, query = await self.invoke(status="pending", metadata=dict(BASE_METADATA, stars_amount=60), stars=90)
        self.assertFalse(result)
        self.assertFalse(query.answer.await_args.kwargs["ok"])


if __name__ == "__main__":
    unittest.main()
