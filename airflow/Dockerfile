# Кастомный образ Airflow с нашими зависимостями.
# Зачем не _PIP_ADDITIONAL_REQUIREMENTS: тот способ Airflow помечает как dev-only и хрупкий —
# переустанавливает пакеты при КАЖДОМ старте (× все airflow-сервисы) и падает, если у пина
# нет готового wheel. Здесь — один раз на билде.
#
# Ставим ТОЛЬКО то, чего нет в базовом образе: clickhouse-connect и scikit-learn.
# pandas / pyarrow / boto3 / httpx apache/airflow:3.2.2 уже несёт сам — не пиним их,
# чтобы не устроить downgrade (именно pyarrow==17.* без wheel под Python 3.13 и падал).
FROM apache/airflow:3.2.2
RUN pip install --no-cache-dir "clickhouse-connect==0.8.*" "scikit-learn==1.5.*"
