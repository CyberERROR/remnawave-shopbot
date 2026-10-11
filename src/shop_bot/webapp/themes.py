from pathlib import Path


WEBAPP_DIR = Path(__file__).resolve().parent
WEBAPP_THEMES_DIR = WEBAPP_DIR / "themes"


def get_available_webapp_themes() -> list[str]:
    themes: list[str] = []
    if not WEBAPP_THEMES_DIR.is_dir():
        return themes
    for theme_dir in sorted(path for path in WEBAPP_THEMES_DIR.iterdir() if path.is_dir()):
        if (theme_dir / "app.html").is_file() and (theme_dir / "login.html").is_file():
            themes.append(theme_dir.name)
    return themes


def resolve_webapp_theme(webapp_settings: dict | None = None) -> str | None:
    from shop_bot.data_manager.remnawave_repository import get_webapp_settings

    settings = webapp_settings or get_webapp_settings()
    themes = get_available_webapp_themes()
    if not themes:
        return None
    selected_theme = (settings.get("webapp_theme") or "").strip()
    if selected_theme in themes:
        return selected_theme
    return themes[0]


def get_webapp_template_path(template_name: str, webapp_settings: dict | None = None) -> Path | None:
    theme_name = resolve_webapp_theme(webapp_settings)
    if not theme_name:
        return None
    template_path = WEBAPP_THEMES_DIR / theme_name / template_name
    return template_path if template_path.is_file() else None
