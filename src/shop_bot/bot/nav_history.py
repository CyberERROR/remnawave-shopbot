user_nav_screens: dict[int, list[str]] = {}
user_nav_current: dict[int, str] = {}

NAV_RESET_CALLBACKS = {"back_to_main_menu", "show_main_menu"}
NAV_SKIP_CALLBACKS = {"nav_back", "noop", "ignore", "admin_cancel", "cancel_promo"}
NAV_SKIP_PREFIXES = ("check_pending:", "check_crypto_invoice:")
NAV_RENDERABLE_SCREENS = {
    "manage_keys", "show_profile", "show_about", "howto_vless",
    "support_menu", "show_help", "buy_new_key", "top_up_start",
    "show_referral_program", "howto_android", "howto_ios",
    "howto_windows", "howto_linux", "support_my_tickets",
    "back_to_main_menu", "show_main_menu",
}
NAV_RENDERABLE_PREFIXES = (
    "show_key_", "key_devices_", "howto_vless_",
    "subscription_addons_", "select_host_new_",
)
NAV_MAX_DEPTH = 12


def is_nav_renderable(callback_data: str | None) -> bool:
    if not callback_data:
        return False
    if callback_data in NAV_RENDERABLE_SCREENS:
        return True
    return callback_data.startswith(NAV_RENDERABLE_PREFIXES)


def reset_nav_screen(user_id: int) -> None:
    user_nav_screens.pop(user_id, None)
    user_nav_current.pop(user_id, None)


def remember_nav_screen(user_id: int, callback_data: str | None) -> None:
    if not user_id or not callback_data:
        return
    if callback_data in NAV_RESET_CALLBACKS:
        reset_nav_screen(user_id)
        return
    if callback_data in NAV_SKIP_CALLBACKS or callback_data.startswith(NAV_SKIP_PREFIXES):
        return
    previous = user_nav_current.get(user_id)
    if previous == callback_data:
        return
    stack = user_nav_screens.get(user_id)
    if stack is None:
        stack = []
        user_nav_screens[user_id] = stack
    if callback_data in stack:
        del stack[stack.index(callback_data):]
    elif is_nav_renderable(previous):
        stack.append(previous)
        if len(stack) > NAV_MAX_DEPTH:
            del stack[0:len(stack) - NAV_MAX_DEPTH]
    user_nav_current[user_id] = callback_data


def set_nav_screen(user_id: int, callback_data: str | None) -> None:
    if not user_id or not callback_data:
        return
    user_nav_current[user_id] = callback_data


def pop_nav_screen(user_id: int) -> str | None:
    stack = user_nav_screens.get(user_id)
    if not stack:
        return None
    return stack.pop()
