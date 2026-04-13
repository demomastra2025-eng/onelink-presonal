# Telegram Personal Gateway

Этот проект запускает MTProto gateway для личного Telegram в OneLink на Python, FastAPI и Telethon.

## Возможности

* Внутренний runtime API для OneLink
* Авторизация личного Telegram через код и 2FA
* Приём личных сообщений, edits, media proxy и исходящая отправка
* Подписанные callbacks обратно в OneLink

## Быстрый старт

### 1. Необходимые условия

* Python 3.11 или новее
* [Poetry](https://python-poetry.org/) для управления зависимостями
* Запущенный экземпляр Chatwoot
* Аккаунт Wasender (для интеграции WhatsApp)
* Telegram-аккаунт (для Telethon, не бот)
* Группа VK и доступ к API

### 2. Установка

Клонируйте репозиторий и установите зависимости:

```bash
git clone git@github.com:feel90d/chatwoot-messenger-gateway.git
cd chatwoot-messenger-gateway
poetry install
````

### 3. Настройка

Скопируйте `.env_template` в `.env` и заполните своими данными:

```bash
cp .env_template .env
```

Отредактируйте `.env` и заполните следующие секции:

* **Chatwoot**:

  + `CHATWOOT_API_ACCESS_TOKEN`
  + `CHATWOOT_ACCOUNT_ID`
  + `CHATWOOT_BASE_URL`
  + `CHATWOOT_WEBHOOK_ID_WHATSAPP`
  + `CHATWOOT_WEBHOOK_ID_TELEGRAM`
  + `CHATWOOT_WEBHOOK_ID_VK`

* **Wasender**:

  + `WASENDER_API_KEY`
  + `WASENDER_WEBHOOK_SECRET`
  + `WASENDER_WEBHOOK_ID`
  + `WASENDER_INBOX_ID`

* **Telegram**:

  + `TG_API_ID`
  + `TG_API_HASH`
  + `TG_SESSION_NAME` (любое имя для вашей сессии Telethon)
  + `TG_INBOX_ID`

* **VK**:

  + `VK_ACCESS_TOKEN`
  + `VK_GROUP_ID`
  + `VK_SECRET`
  + `VK_CONFIRMATION`
  + `VK_API_VERSION`
  + `VK_CALLBACK_ID`
  + `VK_INBOX_ID`

> **Важно:** Все чувствительные значения должны храниться в секрете. Никогда не коммитьте `.env` в публичный репозиторий.

### 4. Запуск приложения

```bash
python ./app/main.py
```

Приложение поднимает FastAPI-сервер с вебхуками для всех настроенных каналов.
Вы можете использовать [ngrok](https://ngrok.com/) или другой туннель, чтобы опубликовать сервер для приёма вебхуков.

Пример:

```bash
ngrok http 8000
```

Обновите URL-адреса вебхуков в Chatwoot, Wasender и VK, чтобы они указывали на публичный адрес ngrok.

### 5. Как это работает

* **Исходящие:** Сообщения из Chatwoot отправляются в WhatsApp, Telegram или VK через соответствующие адаптеры.
* **Входящие:** Ответы пользователей из мессенджеров доставляются в Chatwoot с сохранением всех атрибутов.
* Контакты сопоставляются или создаются на основе идентификаторов мессенджера (например,  `telegram_user_id`,  `telegram_username`).

### 6. Разработка

* Форматирование кода: `poetry run black .`
* Линтинг: `poetry run lint`

## Авторы

* [feel90d](mailto:feel90d@gmail.com), Telegram: [@feel90d](https://t.me/feel90d)
* [lukyan0v\_a](mailto:forjob34@gmail.com), Telegram: [@lukyan0v\_a](https://t.me/lukyan0v_a)

Если у вас есть вопросы, хотите обсудить интеграцию или заинтересованы в похожем решении для вашего бизнеса — пишите нам в Telegram или на почту.
Этот проект создан, чтобы делиться практическим опытом и помогать сообществу — всегда рады новым контактам и обратной связи!

## Лицензия

MIT (или любая другая подходящая вам лицензия)
