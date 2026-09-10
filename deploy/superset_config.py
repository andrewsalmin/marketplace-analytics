# На сервере стенда: /opt/superset/superset_config.py
# Монтируется в контейнер только на чтение, путь задан переменной
# SUPERSET_CONFIG_PATH в docker-compose.yml рядом.
#
# Секретов здесь нет: и SECRET_KEY, и пароль postgres читаются из
# /run/secrets, куда их подаёт docker compose файлами.

import os
import pathlib
import urllib.parse


def _secret(name: str, env_name: str) -> str:
    """Секрет из файла, с откатом на переменную окружения.

    Файл монтируется docker compose в /run/secrets и читается только
    тем процессом, которому нужен. Переменные окружения, в отличие от
    него, видны в docker inspect, лежат в /proc/<pid>/environ и уходят
    в вывод, который вставляют в переписку не глядя.

    Откат оставлен намеренно: если файла нет, сервис поднимется по
    старой схеме, а не упадёт с невнятной ошибкой подключения.
    """
    path = pathlib.Path("/run/secrets") / name
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return os.environ.get(env_name, "")


SECRET_KEY = _secret("superset_secret_key", "SUPERSET_SECRET_KEY")

# quote: пароль попадает внутрь URL, и служебные символы в нём иначе
# разорвали бы строку подключения.
_PG_PASSWORD = urllib.parse.quote(
    _secret("postgres_password", "POSTGRES_PASSWORD"), safe=""
)

SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI") or (
    f"postgresql+psycopg2://superset:{_PG_PASSWORD}@127.0.0.1:5433/superset"
)

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_REDIS_HOST": "127.0.0.1",
    "CACHE_REDIS_PORT": 6380,
    "CACHE_REDIS_DB": 1,
    "CACHE_KEY_PREFIX": "superset_",
}

DATA_CACHE_CONFIG = CACHE_CONFIG

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

AUTH_ROLE_PUBLIC = "Public"

# Разделители чисел по-русски: запятая в дробной части, неразрывный
# пробел в разрядах. Языковой пакет ru в сборку не входит (эндпоинт
# переводов отвечает 404), а вот эта настройка работает — её читает
# фронтенд напрямую.
D3_FORMAT = {
    "decimal": ",",
    "thousands": "\u00a0",
    "grouping": [3],
}

# Список языков, а не перевод интерфейса. Фронтенд берёт язык из
# настроек браузера и ищет его здесь; для браузера с русским языком
# отсутствие "ru" в списке означает languages["ru"].flag по undefined —
# и падение инициализации всего приложения, чёрная страница без единой
# надписи. Языкового пакета ru в сборке нет, интерфейс всё равно будет
# английским, но запись обязана существовать.
LANGUAGES = {
    "en": {"flag": "us", "name": "English"},
    "ru": {"flag": "ru", "name": "Russian"},
}

# Superset стоит за nginx. Без этого он не доверяет заголовкам
# X-Forwarded-* и считает, что его открыли по http на localhost, —
# внутренние ссылки и редиректы строятся с неверной схемой и хостом.
ENABLE_PROXY_FIX = True
