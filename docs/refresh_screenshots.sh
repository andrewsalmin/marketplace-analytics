#!/usr/bin/env bash
#
# Пересъёмка снимков дашборда для README.
#
# Снимки не лежат в репозитории, а отдаются с сервера стенда: README
# ссылается на них по адресу, а не на файл. Причина в том, что данные
# прирастают каждую ночь, и закоммиченная картинка показывает всё более
# старое состояние — а ежедневный коммит двух PNG за год добавил бы
# репозиторию под сотню мегабайт и три сотни коммитов, в которых
# утонула бы настоящая история.
#
# Скрипт зовёт ночная задача (см. dags/daily_marketplace_pipeline.py)
# сразу после обновления дашборда, поэтому на снимке всегда сегодняшние
# числа. Руками он тоже запускается — на сервере стенда:
#
#     docs/refresh_screenshots.sh
#
# Оговорка, которую стоит помнить: картинки в README живут ровно
# столько, сколько живёт стенд. Если стенд когда-нибудь погасят, README
# останется без иллюстраций, и их придётся положить в репозиторий
# файлами.
set -euo pipefail

OUT_DIR="${SCREENSHOT_DIR:-/var/www/marketplace-shots}"
BASE_URL="${SCREENSHOT_URL:-https://marketplace-analytics.andrewsalmin.com}"
DASHBOARD="superset/dashboard/marketplace-overview"

# 90 секунд виртуального времени не «на всякий случай»: на сорока пяти
# графики успевают, а блок выводов остаётся пустым провалом — markdown
# отрисовывается позже данных.
BUDGET_MS=90000

# Пустая или не отрисовавшаяся страница весит килобайты, настоящий
# снимок — сотни. Порог грубый, но ловит именно тот случай, ради
# которого всё и затевалось: картинку без содержимого.
MIN_BYTES=50000

# Снимается публичный адрес, а не localhost: важно увидеть ровно то,
# что увидит посетитель, — вместе с обратным прокси и сертификатом.
shoot() {
    local name="$1" anchor="$2" height="$3"
    # Имя обязано оканчиваться на .png: по расширению Chrome выбирает
    # формат, а при незнакомом молча не пишет ничего.
    local tmp="$OUT_DIR/.$name.new.png"
    local size=0

    # Три попытки: фронтенд Superset изредка не дотягивает один из своих
    #JS-чанков и рисует вместо дашборда «ChunkLoadError». Со второго
    # раза загружается.
    for _ in 1 2 3; do
        google-chrome --headless --disable-gpu --no-sandbox --hide-scrollbars \
            --window-size="1600,$height" \
            --virtual-time-budget="$BUDGET_MS" \
            --screenshot="$tmp" \
            "$BASE_URL/$DASHBOARD/$anchor" >/dev/null 2>&1 || true
        size=$([ -f "$tmp" ] && wc -c < "$tmp" || echo 0)
        [ "$size" -ge "$MIN_BYTES" ] && break
    done

    if [ "$size" -lt "$MIN_BYTES" ]; then
        rm -f "$tmp"
        echo "  ! $name.png весит $size Б — страница не отрисовалась" >&2
        return 1
    fi

    # Подмена одним движением: иначе посетитель README успеет застать
    # файл наполовину записанным.
    mv "$tmp" "$OUT_DIR/$name.png"
    printf '  %-14s %8s Б\n' "$name.png" "$size"
}

echo "Снимаю $BASE_URL в $OUT_DIR"
shoot overview "" 1400
shoot dq "#TAB-dq" 900
