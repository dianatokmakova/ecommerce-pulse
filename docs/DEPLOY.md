# Бесплатный деплой (сервер + Vercel + Yandex DataLens)

Тяжёлый стек разворачивается на собственном сервере, дашборды — в облаках с free-лимитами. Все компоненты используют бесплатные тарифы.

## Карта «что где живёт»

| Слой | Где | Стоимость |
|---|---|---|
| ClickHouse, MinIO, Airflow | сервер, Docker | 0 (собственный сервер) |
| Fake Marketplace API | Vercel (serverless) | 0 (hobby) |
| Лендинг-«витрина» | Vercel (static/Next.js) | 0 |
| Публичный дашборд | Yandex DataLens | 0 (free-лимит) |
| Свой дашборд (ECharts) | Vercel (фронт) + `dashboard-api` на сервере | 0 |
| Код, README, скриншоты | GitHub | 0 |
| HTTPS-сертификат | Let's Encrypt через Caddy | 0 |

> **Vercel ≠ хостинг для Airflow/ClickHouse.** На Vercel размещаются только статика/Next.js и serverless-функции
> (Fake API). Python-дашборд на Streamlit бесплатно хостится на
> **Streamlit Community Cloud** или **Hugging Face Spaces**, не на Vercel.

---

## Шаг 1. Подготовить сервер
- ОС: Ubuntu 22.04/24.04. RAM: от 8 ГБ (минимум 4 ГБ для базового набора сервисов).
- Поставить Docker + Docker Compose v2:
  ```bash
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker $USER   # перелогиниться
  docker compose version
  ```
- Открыть в файрволе только необходимые порты (см. шаг 5). По умолчанию `ufw default deny incoming`.

## Шаг 2. Запустить стек
```bash
git clone <репозиторий> ecommerce-pulse && cd ecommerce-pulse
cp .env.example .env && nano .env          # задать сильные пароли
# указать креды MinIO в infra/clickhouse/config.d/memory.xml (named collection)

docker compose up -d clickhouse minio createbuckets postgres \
    airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor

docker compose ps                          # все сервисы должны быть healthy
```
DDL `infra/clickhouse/init/*.sql` применяется автоматически при первом старте ClickHouse
(базы bronze/silver/gold + пользователь `bi_reader`).

## Шаг 3. Залить данные и включить пайплайны
1. Скачать Olist с Kaggle, положить 9 CSV в MinIO bucket `raw/olist/` (через консоль `:9001` или `mc`).
2. В Airflow UI (`:8080`) включить (unpause) DAG `olist_batch_load` → дождаться зелёного рана.
3. Задеплоить Fake API (шаг 4), прописать `FAKE_API_URL` в `.env`, перезапустить Airflow-сервисы,
   включить DAG `live_pricing_ingest`.

## Шаг 4. Fake Marketplace API на Vercel
```bash
cd fake-marketplace-api
npm i -g vercel
vercel            # залогиниться, задеплоить (free hobby)
vercel --prod     # получить прод-URL вида https://<project>.vercel.app
# проверка:
curl https://<project>.vercel.app/api/prices | head
```
Полученный URL (с `/api/prices`) указывается в `.env` → `FAKE_API_URL`.

## Шаг 5. HTTPS и безопасность (ClickHouse публикуется только через Caddy для DataLens)
- Caddy (см. `infra/caddy/Caddyfile`) автоматически получает сертификат Let's Encrypt для домена.
- Наружу открыты только `80/443` (Caddy) — единственный публичный сервис. Порт `8123` ClickHouse
  и остальные порты привязаны к `localhost` и напрямую не публикуются.
- Read-only пользователь `bi_reader` (уже в DDL) — BI подключается только им.
- В `ufw` открыты: `22` (SSH, ограничить доверенным IP), `80`, `443`. Остальное — `deny`.
- Для DataLens в whitelist добавляются его **диапазоны IP** (список — в документации DataLens).
- Консоли Airflow/MinIO не публикуются открыто: basic-auth через Caddy либо доступ по SSH-туннелю
  (`ssh -L 8080:localhost:8080 user@server`).

## Шаг 6. DataLens (публичный дашборд)
См. `datalens/README.md`. Кратко: Connection → ClickHouse (`ch.example.com`, HTTPS, `bi_reader`) →
датасеты на gold → дашборд → **DataLens Public** → публичная ссылка.

## Шаг 7. Свой дашборд (ECharts на Vercel + `dashboard-api`)
`dashboard-api` поднимается вместе со стеком (`docker compose up -d dashboard-api`, порт `:8000`);
в проде закрывается HTTPS через Caddy (фронт на Vercel требует https). Фронт — статическая папка
`dashboard/`: `cd dashboard && vercel --prod`. Домен API задаётся в `dashboard/index.html`
(const `API`) или через query-параметр `?api=https://api.<домен>`.

## Шаг 8. Лендинг-витрина (опц., Vercel)
Одностраничник (HTML или Next.js) со схемой архитектуры и кнопками: «Дашборд DataLens»,
«Свой дашборд», «GitHub». Деплой `vercel --prod`.

---

## Чек-лист «всё взлетело»
- [ ] `docker compose ps` — все сервисы healthy.
- [ ] `olist_batch_load` отработал; в `gold.*` есть строки.
- [ ] `live_pricing_ingest` идёт по часам; `bronze.pricing_raw` растёт; есть строки в `gold.pricing_anomalies`.
- [ ] DataLens-дашборд открывается по публичной ссылке.
- [ ] Свой дашборд развёрнут на Vercel и читает gold-витрины через `dashboard-api`.
- [ ] ClickHouse только за HTTPS; `8123` напрямую закрыт; консоли не публичны.
- [ ] README с диаграммой, скриншотами и ссылками готов.

## Частые грабли
- **Airflow «съел» память / контейнеры рестартятся** → мало RAM. Выделить Docker ≥4 ГБ или запускать сокращённый набор сервисов.
- **ClickHouse не читает MinIO** → проверить named collection в `config.d/memory.xml` и хост `minio:9000` (внутри сети), а не `localhost`.
- **DataLens не коннектится** → IP DataLens не добавлены в whitelist / нет HTTPS.
- **Конфликт порта 9000** → MinIO наружу на `9002` (внутри всё равно `minio:9000`), см. комментарий в `docker-compose.yml`.
