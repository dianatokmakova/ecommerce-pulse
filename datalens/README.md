# Дашборд на Yandex DataLens (основной, публичный)

DataLens — BI от Yandex. Подключается к внешнему ClickHouse этого проекта и публикуется наружу одной ссылкой.

## Шаги
1. Создать аккаунт **Yandex Cloud** и **DataLens**. Требуется биллинг-аккаунт с привязанной картой,
   при небольшом объёме использование DataLens бесплатно.
2. **Подключение (Connection) → ClickHouse**:
   - Host: публичный адрес сервера (`ch.example.com`, лучше за HTTPS через Caddy);
   - Port: `8443` (HTTPS) / `8123`;
   - User: `bi_reader` (read-only), пароль;
   - включить HTTPS; при необходимости задать Cache TTL.
3. **Whitelist IP (обязательно).** Для внешнего (не Yandex.Cloud) источника на файрволе нужно открыть
   диапазоны IP DataLens (актуальный список — в документации DataLens). Остальное — `deny`.
4. **Датасеты** поверх gold-витрин: `gold.daily_revenue`, `gold.rfm`, `gold.cohort_retention`,
   `gold.delivery_sla`, `gold.pricing_anomalies`.
5. **Чарты и дашборд** (минимум 5–6):
   - линия выручки по месяцам + AOV;
   - бар: топ категорий / штатов;
   - **heatmap когорт** (cohort_month × month_number → retention_rate);
   - таблица/бар RFM-сегментов;
   - **карта** Бразилии по штатам (выручка/просрочки);
   - alert-таблица «аномалии цен сейчас» из `gold.pricing_anomalies WHERE is_anomaly=1`.
6. **Публикация.** Включить **DataLens Public** — будет получена публичная ссылка.

## Безопасность
- Только read-only пользователь (`bi_reader`), без прав на запись/DROP.
- ClickHouse наружу — только по HTTPS и только для нужных IP.
- Консоли Airflow/MinIO не публикуются в открытый интернет (см. `infra/caddy/Caddyfile`).

> Два BI-слоя: **DataLens** — публичный exec-дашборд; свой кастом-дашборд
> (`dashboard/` на Vercel + `dashboard-api`) — детальный анализ поверх gold-витрин.
