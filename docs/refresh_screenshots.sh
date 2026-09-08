#!/usr/bin/env bash
#
# Пересъёмка скриншотов для README.
#
# Данные прирастают каждую ночь, поэтому картинки показывают всё более
# старое состояние. Обновлять их по расписанию не стоит: ежедневный
# коммит двух PNG за год добавит репозиторию сотню мегабайт и триста
# коммитов, в которых утонет настоящая история. Поэтому — руками, перед
# тем как показывать проект:
#
#     docs/refresh_screenshots.sh
#     git add docs/*.png && git commit -m "docs: refresh dashboard screenshots"
#
# Съёмка идёт на сервере стенда, а не на рабочей машине. Причина
# прозаическая: там стоит Chrome, который headless умеет, а Edge на
# Windows в этом режиме падает, не создавая файла. Снимки копируются
# обратно, коммит делается здесь.
#
# Адрес сервера — публичное имя дашборда, то же, что в README. Хост
# можно переопределить: SCREENSHOT_HOST=user@example.com docs/refresh_screenshots.sh
set -euo pipefail

REMOTE="${SCREENSHOT_HOST:-andrewsalmin@marketplace-analytics.andrewsalmin.com}"
BASE_URL="${SCREENSHOT_URL:-https://marketplace-analytics.andrewsalmin.com}"
DASHBOARD="superset/dashboard/marketplace-overview"
OUT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_DIR="/tmp/marketplace-shots"

# Снимается публичный адрес, а не localhost: важно увидеть ровно то, что
# увидит посетитель — вместе с обратным прокси и сертификатом.
#
# 90 секунд виртуального времени не «на всякий случай»: на сорока пяти
# графики успевают, а блок выводов остаётся пустым провалом — markdown
# отрисовывается позже данных.
shoot_remote() {
    local name="$1" anchor="$2" height="$3"
    ssh "$REMOTE" "mkdir -p '$REMOTE_DIR' && google-chrome \
        --headless --disable-gpu --no-sandbox --hide-scrollbars \
        --window-size='1600,$height' \
        --virtual-time-budget=90000 \
        --screenshot='$REMOTE_DIR/$name.png' \
        '$BASE_URL/$DASHBOARD/$anchor'" >/dev/null 2>&1
    scp -q "$REMOTE:$REMOTE_DIR/$name.png" "$OUT_DIR/$name.png"

    local size
    size=$(wc -c < "$OUT_DIR/$name.png")
    # Пустая или не отрисовавшаяся страница весит килобайты, настоящий
    # снимок — сотни. Проверка грубая, но ловит именно тот случай, ради
    # которого всё и затевалось: картинку без содержимого.
    if [ "$size" -lt 50000 ]; then
        echo "  ! $name.png весит $size Б — похоже, страница не отрисовалась" >&2
        return 1
    fi
    printf '  %-14s %8s Б\n' "$name.png" "$size"
}

echo "Снимаю $BASE_URL (через $REMOTE)"
shoot_remote overview "" 1400
shoot_remote dq "#TAB-dq" 900
ssh "$REMOTE" "rm -rf '$REMOTE_DIR'" >/dev/null 2>&1 || true

echo
echo "Готово. Посмотри картинки глазами — проверка по весу ловит только"
echo "пустую страницу, но не наполовину прогрузившуюся. Затем:"
echo "  git add docs/*.png && git commit -m 'docs: refresh dashboard screenshots'"
