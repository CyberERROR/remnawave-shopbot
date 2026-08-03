from shop_bot.data_manager.remnawave_repository import get_setting


def get_available_payment_methods(include_balance: bool = False, balance: float | None = None) -> list[dict]:
    methods: list[dict] = []

    if include_balance:
        suffix = ""
        if balance is not None:
            try:
                suffix = f" ({balance:.0f} RUB)"
            except Exception:
                pass
        methods.append({
            "method": "balance",
            "purchase_callback": "pay_balance",
            "topup_callback": None,
            "webapp_id": "pay_balance",
            "bot_default_label": "💼 Оплатить с баланса",
            "webapp_default_label": "Баланс",
            "icon": "account_balance",
            "suffix": suffix,
        })

    yookassa_enabled = bool((get_setting("yookassa_shop_id") or "") and (get_setting("yookassa_secret_key") or ""))
    if yookassa_enabled:
        sbp_enabled = (get_setting("sbp_enabled") or "false").strip().lower() == "true"
        methods.append({
            "method": "yookassa",
            "purchase_callback": "pay_yookassa",
            "topup_callback": "topup_pay_yookassa",
            "webapp_id": "pay_yookassa",
            "bot_default_label": "🏦 СБП / Банковская карта" if sbp_enabled else "🏦 Банковская карта",
            "webapp_default_label": "СБП / Банковская карта" if sbp_enabled else "Банковская карта",
            "icon": "credit_card",
            "suffix": "",
        })

    ordered_methods = [
        ("platega_payform", "platega_payform_enabled", "pay_platega_payform", "topup_pay_platega_payform", "💳 Platega", "Platega", "credit_card"),
        ("platega", "platega_enabled", "pay_platega", "topup_pay_platega", "💳 СБП / Platega", "СБП / Platega", "payments"),
        ("platega_crypto", "platega_crypto_enabled", "pay_platega_crypto", "topup_pay_platega_crypto", "🪙 Crypto / Platega", "Крипта / Platega", "payments"),
        ("tonconnect", None, "pay_tonconnect", "topup_pay_tonconnect", "🪙 TON Connect", "TON Connect", "wallet"),
        ("stars", "stars_enabled", "pay_stars", "topup_pay_stars", "⭐ Telegram Stars", "Telegram Stars", "star"),
        ("yoomoney", "yoomoney_enabled", "pay_yoomoney", "topup_pay_yoomoney", "💜 ЮMoney (кошелёк)", "ЮMoney (кошелёк)", "account_balance_wallet"),
    ]

    for method, enabled_key, purchase_callback, topup_callback, bot_default_label, webapp_default_label, icon in ordered_methods[:3]:
        if (get_setting(enabled_key) or "false").strip().lower() == "true":
            methods.append({
                "method": method,
                "purchase_callback": purchase_callback,
                "topup_callback": topup_callback,
                "webapp_id": purchase_callback,
                "bot_default_label": bot_default_label,
                "webapp_default_label": webapp_default_label,
                "icon": icon,
                "suffix": "",
            })

    cryptobot_enabled = bool(get_setting("cryptobot_token") or "")
    heleket_enabled = bool((get_setting("heleket_merchant_id") or "") and (get_setting("heleket_api_key") or ""))
    if cryptobot_enabled:
        methods.append({
            "method": "cryptobot",
            "purchase_callback": "pay_cryptobot",
            "topup_callback": "topup_pay_cryptobot",
            "webapp_id": "pay_cryptobot",
            "bot_default_label": "💎 Криптовалюта",
            "webapp_default_label": "Криптовалюта",
            "icon": "currency_bitcoin",
            "suffix": "",
        })
    elif heleket_enabled:
        methods.append({
            "method": "heleket",
            "purchase_callback": "pay_heleket",
            "topup_callback": "topup_pay_heleket",
            "webapp_id": "pay_heleket",
            "bot_default_label": "💎 Криптовалюта",
            "webapp_default_label": "Криптовалюта",
            "icon": "currency_bitcoin",
            "suffix": "",
        })

    for method, enabled_key, purchase_callback, topup_callback, bot_default_label, webapp_default_label, icon in ordered_methods[3:]:
        enabled = False
        if method == "tonconnect":
            enabled = bool((get_setting("ton_wallet_address") or "") and (get_setting("tonapi_key") or ""))
        else:
            enabled = (get_setting(enabled_key) or "false").strip().lower() == "true"
        if enabled:
            methods.append({
                "method": method,
                "purchase_callback": purchase_callback,
                "topup_callback": topup_callback,
                "webapp_id": purchase_callback,
                "bot_default_label": bot_default_label,
                "webapp_default_label": webapp_default_label,
                "icon": icon,
                "suffix": "",
            })

    return methods
