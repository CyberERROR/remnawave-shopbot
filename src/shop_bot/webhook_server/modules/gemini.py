from flask import Blueprint, request, jsonify, render_template
from shop_bot.data_manager.database import get_setting, update_setting, get_other_setting, update_other_setting
import logging
import httpx
from datetime import datetime

logger = logging.getLogger(__name__)

# Полная база знаний по умолчанию (используется ТОЛЬКО если в БД нет промта)
DEFAULT_KNOWLEDGE_BASE = (
    "Ты виртуальный помощник технической поддержки VPN-сервиса.\n\n"
    "=== СТРУКТУРА СЕРВИСА ===\n"
    "ГЛАВНОЕ МЕНЮ содержит разделы:\n"
    "- 🏠 Главное меню - возврат в основное меню\n"
    "- 👤 Профиль - информация об аккаунте и подписке\n"
    "- 🛒 Купить - переход к покупке подписки\n"
    "- 🆘 Поддержка - обращение в техподдержку\n"
    "- 🔑 Мои ключи - доступ к конфигурационным файлам подключения\n"
    "- ℹ️ О проекте - информация о сервисе\n\n"
    
    "=== ПОКУПКА НОВОГО КЛЮЧА ===\n"
    "Раздел 'Купить' → Выбор сервера → Выбор тарифа\n"
    "Пользователю нужно:\n"
    "1. Выбрать подходящий тип сервера (в зависимости от количества устройств)\n"
    "2. Выбрать нужный тариф на нужный срок (доступны разные периоды подписки)\n\n"
    
    "=== ПРОДЛЕНИЕ СУЩЕСТВУЮЩЕГО КЛЮЧА ===\n"
    "Раздел 'Мои ключи' → Выбор ключа → 'Продлить этот ключ'\n"
    "Пользователю нужно выбрать нужный тариф на нужный срок для продления.\n\n"
    
    "РАЗДЕЛ 'МОИ КЛЮЧИ' (МОИ ПОДПИСКИ):\n"
    "Показывает список всех активных подписок пользователя:\n"
    "- Номер ключа (например, #1, #2)\n"
    "- Тип сервера\n"
    "- Срок действия в днях (например: 30 дней)\n"
    "- Статус активности (✅ активен)\n"
    "Действия:\n"
    "- 🛒 Купить ключ - покупка новой подписки\n"
    "- 🔙 Назад в меню\n\n"
    
    "ИНФОРМАЦИЯ О ПОДПИСКЕ (при выборе ключа):\n"
    "- 🔑 Номер ключа\n"
    "- 📅 Приобретен: дата и время\n"
    "- ⏱ Действителен до: дата и время\n"
    "- 🔗 Ссылка на подписку\n"
    "Действия с ключом:\n"
    "- 🔌 Подключиться - получение ссылки подписки для подключения\n"
    "- ➕ Продлить этот ключ - продление конкретной подписки\n"
    "- 📱 Показать QR-код - QR для быстрого подключения\n"
    "- 📖 Инструкция - подробная инструкция по подключению для разных платформ\n"
    "- 🔙 Назад к списку ключей\n\n"
    
    "ИНСТРУКЦИЯ ПО ПОДКЛЮЧЕНИЮ:\n"
    "Выбор платформы для получения инструкции:\n"
    "- 📱 Android\n"
    "- 🍎 iOS\n"
    "- 💻 Windows\n"
    "- 🐧 Linux\n"
    "После выбора платформы предоставляется пошаговая инструкция по настройке VLESS подключения\n"
    "- 🔙 Назад к ключу\n\n"
    
    "РАЗДЕЛ 'ПРОФИЛЬ' показывает:\n"
    "- 👤 Профиль пользователя (username)\n"
    "- 💰 Потрачено всего (в RUB)\n"
    "- 📅 Приобретено месяцев (общая статистика)\n"
    "- ✅ Статус VPN (Активен/Неактивен)\n"
    "- ⏱ Осталось времени подписки (дни и часы)\n"
    "- 💎 Основной баланс (в RUB)\n"
    "- 🤝 Рефераль (количество рефералов)\n"
    "- 💵 Заработано по реферралке (всего в RUB)\n"
    "Действия в профиле:\n"
    "- 💳 Пополнить баланс - пополнение внутреннего баланса для оплаты подписок\n"
    "- 🤝 Реферальная программа - получение реферальной ссылки и статистики\n"
    "- 🔙 Назад в меню\n\n"
    
    "ПОПОЛНЕНИЕ БАЛАНСА:\n"
    "- Примерная сумма пополнения: 200 RUB\n"
    "- Максимальная сумма: без ограничений\n"
    "- После ввода суммы - выбор способа оплаты (СБП/Карта, Криптовалюта, Telegram Stars)\n"
    "- Средства на балансе\n\n"
    
    "ВАЖНЫЕ ПРАВИЛА ГЕНЕРАЦИИ ОТВЕТА:\n"
    "1. ЭТО ОТВЕТ ТЕХНИЧЕСКОЙ ПОДДЕРЖКИ В TELEGRAM ЧАТЕ.\n"
    "2. МАКСИМАЛЬНАЯ КРАТКОСТЬ. Если вопрос простой — ответ должен быть в 1 предложение. НЕ ЛЕЙ ВОДУ.\n"
    "3. НЕ ПОВТОРЯЙСЯ. Если запрос уже принят — не пиши это снова.\n"
    "4. СТРОГО БЕЗ Markdown (bold, italic) и HTML. ПИШИ ПРОСТЫМ ТЕКСТОМ.\n"
    "5. СТИЛЬ: ЖИВОЙ, ПРОФЕССИОНАЛЬНЫЙ, НО ПРОСТОЙ.\n"
    "6. ИЗБЕГАЙ 'ИИ-МАРКЕРОВ'.\n"
    "7. ЭМОДЗИ: умеренно (1-2 на сообщение)."
)

def _build_final_prompt(db_prompt, messages, current_draft):
    """
    Строит финальный промт для Gemini.
    Если db_prompt задан -> используем его как базу (подразумевается, что админ сам настроил правила).
    Если db_prompt пуст -> используем DEFAULT_KNOWLEDGE_BASE (с дефолтными правилами).
    В обоих случаях добавляем контекст, историю и инструкцию.
    """
    
    # 1. Выбираем базу
    if db_prompt and db_prompt.strip():
        base_system_content = db_prompt.strip()
    else:
        base_system_content = DEFAULT_KNOWLEDGE_BASE

    # 2. Формируем историю чата
    if not messages:
        history_text = "История чата пуста."
    else:
        history_lines = []
        for m in messages:
            sender = "Admin" if m['sender'] == 'admin' else "User"
            content = m['content'] or ""
            history_lines.append(f"{sender}: {content}")
        history_text = "\n".join(history_lines)

    # 3. Формируем инструкцию по задаче
    instruction_part = "Сформулируй ответ пользователю (от имени поддержки) на основе последнего сообщения и контекста."
    user_input_part = ""

    if current_draft:
        user_input_part = f"Вот черновик ответа от оператора: \"{current_draft}\""
        instruction_part = "Твоя задача: ОТРЕДАКТИРОВАТЬ этот черновик. Исправь ошибки и сделай его вежливее, но СТРОГО СОХРАНИ КРАТКОСТЬ И СМЫСЛ."

    # 4. Правило приветствия
    has_admin_reply = any(m.get('sender') == 'admin' for m in messages)
    greeting_rule = "Обязательно начни с приветствия." if not has_admin_reply else "Без приветствий."

    # 5. Сборка финального промта
    final_prompt = (
        f"{base_system_content}\n\n"
        f"=== КОНТЕКСТ ===\n"
        f"История чата:\n{history_text}\n\n"
        f"{user_input_part}\n"
        f"{instruction_part}\n"
        f"Правило приветствия: {greeting_rule}"
    )
    
    return final_prompt

def _build_api_base(base_url):
    """
    Приводит Base URL к формату OpenAI SDK (без /chat/completions и /models).
    """
    url = (base_url or "").strip().rstrip("/")
    if not url:
        return "https://generativelanguage.googleapis.com/v1beta/openai"
    if url.endswith("/chat/completions"):
        url = url[:-len("/chat/completions")]
    elif url.endswith("/models"):
        url = url[:-len("/models")]
    last_segment = url.rsplit("/", 1)[-1]
    if last_segment == "openai" or (last_segment.startswith("v") and last_segment[1:].isdigit()):
        return url
    return url + "/v1"

def _build_chat_completions_url(base_url):
    """
    Формирует endpoint /chat/completions из Base URL.
    """
    return _build_api_base(base_url) + "/chat/completions"

def _build_models_url(base_url):
    """
    Формирует endpoint /models из Base URL.
    """
    return _build_api_base(base_url) + "/models"

def _call_ai_api(api_key, base_url, model, prompt):
    """
    Выполняет запрос к OpenAI-совместимому AI API.
    """
    url = _build_chat_completions_url(base_url)
    payload = {
        "model": (model or "").strip() or "gemini-2.5-flash",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload, headers=headers)
    
    if resp.status_code != 200:
        logger.error(f"AI API Error: {resp.status_code} {resp.text}")
        raise Exception(f"AI API Error: {resp.status_code}")
    
    resp_data = resp.json()
    try:
        choices = resp_data.get('choices', [])
        if not choices:
            raise Exception("No choices returned from AI")
        
        return choices[0].get('message', {}).get('content', '') or ''
    except Exception as e:
        logger.error(f"Error parsing AI response: {e}")
        raise Exception("Failed to parse AI response")

def register_gemini_routes(app, login_required):
    """
    Registers the routes for Gemini API key management and generation.
    """
    
    # Универсальный обработчик для настроек (GET/POST)
    def handle_server_gemini_settings():
        try:
            if request.method == 'POST':
                data = request.get_json(silent=True)
                if not data:
                     return jsonify({'ok': False, 'error': 'No data provided'}), 400
                
                new_key = data.get('key')
                new_base_url = data.get('base_url')
                new_model = data.get('model')
                new_prompt = data.get('prompt')
                support_enabled = data.get('support_enabled')
                schedule_enabled = data.get('schedule_enabled')
                schedule_start = data.get('schedule_start')
                schedule_end = data.get('schedule_end')

                if schedule_start is not None or schedule_end is not None:
                    try:
                        datetime.strptime(schedule_start or '', '%H:%M')
                        datetime.strptime(schedule_end or '', '%H:%M')
                    except ValueError:
                        return jsonify({'ok': False, 'error': 'Invalid support schedule'}), 400

                if new_key is not None:
                    update_setting('key_gemini', new_key)
                if new_base_url is not None:
                    update_setting('gemini_base_url', new_base_url)
                if new_model is not None:
                    update_setting('gemini_model', new_model)
                if new_prompt is not None:
                    update_other_setting('sg_promt', new_prompt)
                if support_enabled is not None:
                    update_setting('support_enabled', 'true' if support_enabled else 'false')
                if schedule_enabled is not None:
                    update_setting('support_schedule_enabled', 'true' if schedule_enabled else 'false')
                if schedule_start is not None:
                    update_setting('support_schedule_start', schedule_start)
                if schedule_end is not None:
                    update_setting('support_schedule_end', schedule_end)
                    
                return jsonify({'ok': True})
            else:
                key = get_setting('key_gemini')
                prompt = get_other_setting('sg_promt', "")
                return jsonify({
                    'ok': True,
                    'key': key,
                    'base_url': get_setting('gemini_base_url') or '',
                    'model': get_setting('gemini_model') or '',
                    'prompt': prompt,
                    'support_enabled': get_setting('support_enabled') != 'false',
                    'schedule_enabled': get_setting('support_schedule_enabled') == 'true',
                    'schedule_start': get_setting('support_schedule_start') or '10:00',
                    'schedule_end': get_setting('support_schedule_end') or '23:00',
                })
        except Exception as e:
            logger.error(f"Error in Gemini settings: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    # Регистрируем единый обработчик на разные пути для совместимости
    app.route('/admin/gemini/settings', methods=['GET', 'POST'], endpoint='gemini_settings')(login_required(handle_server_gemini_settings))
    app.route('/admin/gemini/key', methods=['GET', 'POST'], endpoint='gemini_key')(login_required(handle_server_gemini_settings))

    @app.route('/admin/gemini/generate', methods=['POST'])
    @login_required
    def generate_gemini_response():
        try:
            data = request.get_json(silent=True)
            if not data or 'ticket_id' not in data:
                return jsonify({'ok': False, 'error': 'Missing ticket_id'}), 400
            
            # Получаем ключ
            api_key = get_setting('key_gemini')
            if not api_key:
                 return jsonify({'ok': False, 'error': 'API Key not configured'}), 400
            
            base_url = get_setting('gemini_base_url') or ''
            model = get_setting('gemini_model') or ''
            
            # Получаем сообщения тикета
            from shop_bot.data_manager.database import get_ticket_messages
            messages = get_ticket_messages(data['ticket_id'])
            
            # Получаем настройки промта
            db_prompt = get_other_setting('sg_promt')
            
            # Строим финальный промт
            final_prompt = _build_final_prompt(
                db_prompt=db_prompt,
                messages=messages,
                current_draft=data.get('current_text', '')
            )
            
            # Делаем запрос
            generated_text = _call_ai_api(api_key, base_url, model, final_prompt)
            return jsonify({'ok': True, 'response': generated_text})

        except Exception as e:
            logger.error(f"Error generating Gemini response: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/admin/gemini/models', methods=['POST'], endpoint='gemini_models')
    @login_required
    def fetch_gemini_models():
        try:
            data = request.get_json(silent=True) or {}
            api_key = (data.get('key') or '').strip() or get_setting('key_gemini')
            if not api_key:
                return jsonify({'ok': False, 'error': 'API Key not configured'}), 400

            base_url = data.get('base_url')
            if base_url is None:
                base_url = get_setting('gemini_base_url') or ''

            url = _build_models_url(base_url)
            headers = {"Authorization": f"Bearer {api_key}"}

            with httpx.Client(timeout=30.0) as client:
                resp = client.get(url, headers=headers)

            if resp.status_code != 200:
                logger.error(f"AI Models API Error: {resp.status_code} {resp.text}")
                return jsonify({'ok': False, 'error': f'AI API Error: {resp.status_code}'}), 502

            resp_data = resp.json()
            models = []
            for item in resp_data.get('data') or []:
                model_id = item.get('id') if isinstance(item, dict) else None
                if model_id:
                    models.append(model_id)
            return jsonify({'ok': True, 'models': models})

        except Exception as e:
            logger.error(f"Error fetching AI models: {e}")
            return jsonify({'ok': False, 'error': str(e)}), 500
