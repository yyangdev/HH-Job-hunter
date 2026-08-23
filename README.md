
# tdata-to-session

[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-ready--to--code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/yyangdev/tdata-to-session)
[![Discord](https://img.shields.io/badge/Discord-chat-5865F2?logo=discord&logoColor=white)](https://discord.gg/your-invite)
[![Gitter](https://img.shields.io/badge/Gitter-chat-46BC99?logo=gitter&logoColor=white)](https://gitter.im/your-room)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/yyangdev/tdata-to-session/ci.yml?branch=main&label=CI)](https://github.com/yyangdev/tdata-to-session/actions)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/yyangdev/tdata-to-session/blob/main/.pre-commit-config.yaml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Конвертер папки `tdata` (Telegram Desktop) в `.session` для Telethon.**

Работает как CLI-утилита и как Python-библиотека. Превращает сессии из десктопного Telegram в формат, который можно использовать в скриптах и ботах.

---

## 📦 Установка

```bash
pip install opentele2 telethon
```

**Требуется Python 3.8+**

---

## 🚀 Использование (CLI)

### Базовый запуск

```bash
python main.py --tdata ./tdata --phone +79991234567
```

### С прокси

```bash
python main.py --tdata ./tdata --phone +79991234567 --proxy socks5://user:pass@host:1080
```

### Все опции

| Аргумент | Обязательный | Описание |
|----------|--------------|----------|
| `--tdata` | Да | Путь к папке `tdata` |
| `--phone` | Нет | Номер телефона в формате `+79991234567`. Если не указан, парсится из имени папки. |
| `--proxy` | Нет | Прокси в формате `socks5://user:pass@host:port` |
| `--timeout` | Нет | Таймаут операций в секундах. По умолчанию: `60` |
| `--verbose` | Нет | Включить подробный вывод логов |

---

## 📁 Использование (Python)

### Конвертация одной папки

```python
import asyncio
from pathlib import Path
from tdata_converter import TDataConverter

async def main():
    converter = TDataConverter()
    success, result = await converter.convert_folder(
        Path("./tdata"),
        phone="+79991234567"
    )

    if success:
        print(f"Сессия сохранена: {result['session_file']}")
        print(f"Аккаунт: {result['user']['first_name']} (@{result['user']['username']})")

asyncio.run(main())
```

### Конвертация ZIP-архива (для веб-интерфейсов)

```python
from fastapi import UploadFile
from tdata_converter import TDataConverter

converter = TDataConverter()
results = await converter.convert_zip_batch(zip_file=uploaded_zip)

# results = {
#     "success": [{"phone": "+7999...", "session_file": "...", "user": {...}}, ...],
#     "failed": [{"phone": "...", "error": "..."}, ...],
#     "total": 5
# }
```

### Массовая конвертация папок

```python
folders = [
    (Path("./tdata1"), "+79991234567"),
    (Path("./tdata2"), "+79991234568"),
]
results = await converter.convert_folder_batch(folders)
```

---

## ⚙️ Конфигурация через `.env`

Создайте файл `.env` в корне проекта:

```env
API_ID=123456
API_HASH=your_api_hash_here
PROXY=socks5://user:pass@host:1080
```

Если `.env` не найден, используются переменные окружения `API_ID` и `API_HASH`.

---

## 📂 Структура результата

- `sessions/+79991234567.session` — файл сессии для Telethon
- `sessions/+79991234567.meta` — JSON с данными аккаунта:
  ```json
  {
    "phone": "+79991234567",
    "id": 123456789,
    "username": "username",
    "first_name": "Имя",
    "last_name": "Фамилия",
    "premium": false,
    "created": "2026-08-23T12:00:00"
  }
  ```

---

## 🧠 Как это работает

1. **Загрузка `tdata`** — библиотека `opentele2` читает зашифрованные ключи сессии из папки `tdata`.
2. **Создание клиента Telethon** — на основе прочитанных данных создается `TelegramClient` с пустой `StringSession`.
3. **Авторизация** — клиент подключается к Telegram и проверяет, авторизован ли аккаунт.
4. **Сохранение сессии** — полученная строка сессии записывается в файл `.session` с именем телефона.
5. **Метаданные** — дополнительно сохраняется информация об аккаунте для быстрого доступа.

---

## 🧹 Очистка кэша

```python
converter.clear_cache()
```

## 📄 Лицензия

MIT. Используйте как угодно.
