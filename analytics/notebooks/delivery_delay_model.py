# %% [markdown]
# # Прогноз просрочки доставки
#
# Pandas / NumPy для EDA + базовый supervised-ML: предсказание, опоздает ли заказ
# (`delay_days > 0`) по признакам, известным в момент покупки. Данные — из ClickHouse silver.

# %%
import os
import numpy as np
import pandas as pd
import clickhouse_connect
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix

ch = clickhouse_connect.get_client(
    host=os.environ.get("CH_HOST", "localhost"),
    port=int(os.environ.get("CH_PORT", "8123")),
    username=os.environ.get("CH_USER", "analyst"),
    password=os.environ.get("CH_PASSWORD", "change_me_strong_password"),
)

# %% [markdown]
# ## 1. Загрузка — агрегируем до уровня заказа
# Один заказ = одна строка с признаками (категория, стоимость, фрахт, вес, сезонность).

# %%
df = ch.query_df("""
    SELECT
        o.order_id                         AS order_id,
        ifNull(c.state, 'NA')              AS state,
        any(ifNull(p.category, 'unknown')) AS category,
        round(sum(oi.price), 2)            AS order_value,
        round(sum(oi.freight_value), 2)    AS freight,
        count()                            AS n_items,
        avg(p.weight_g)                    AS avg_weight,
        toMonth(o.purchase_ts)             AS month,
        toDayOfWeek(o.purchase_ts)         AS dow,
        o.delay_days                       AS delay_days
    FROM silver.orders      AS o
    INNER JOIN silver.order_items AS oi ON oi.order_id = o.order_id
    LEFT  JOIN silver.products    AS p  ON p.product_id = oi.product_id
    LEFT  JOIN silver.customers   AS c  ON c.customer_id = o.customer_id
    WHERE o.is_delivered = 1 AND o.delay_days IS NOT NULL
    GROUP BY o.order_id, state, o.delay_days, o.purchase_ts
""")
print(f"{len(df):,} заказов загружено")

# %% [markdown]
# ## 2. EDA — NumPy/Pandas

# %%
df["late"] = (df["delay_days"] > 0).astype(int)
df["avg_weight"] = df["avg_weight"].fillna(df["avg_weight"].median())
print("доля просрочек:", round(df["late"].mean(), 3))
print(df.groupby("late")[["order_value", "freight", "n_items", "avg_weight"]].mean().round(2))
# топ штатов по просрочке (минимум 100 заказов)
g = df.groupby("state").agg(orders=("late", "size"), late_rate=("late", "mean"))
print(g[g.orders > 100].sort_values("late_rate", ascending=False).head(10).round(3))

# %% [markdown]
# ## 3. Модель — train/test, baseline (LogReg) vs RandomForest

# %%
NUM = ["order_value", "freight", "n_items", "avg_weight", "month", "dow"]
CAT = ["state", "category"]
X, y = df[NUM + CAT], df["late"]
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

pre = ColumnTransformer([
    ("num", StandardScaler(), NUM),
    ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=50), CAT),
])

models = {
    "logreg": LogisticRegression(max_iter=1000, class_weight="balanced"),
    "rf": RandomForestClassifier(n_estimators=200, max_depth=12,
                                 class_weight="balanced", n_jobs=-1, random_state=42),
}
fitted = {}
for name, clf in models.items():
    pipe = Pipeline([("pre", pre), ("clf", clf)]).fit(X_tr, y_tr)
    proba = pipe.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, proba)
    print(f"\n=== {name} · ROC-AUC={auc:.3f} ===")
    print(classification_report(y_te, (proba > 0.5).astype(int), digits=3))
    print("confusion:\n", confusion_matrix(y_te, (proba > 0.5).astype(int)))
    fitted[name] = pipe

# %% [markdown]
# ## 4. Важность признаков (RandomForest) + график

# %%
rf = fitted["rf"]
names = rf.named_steps["pre"].get_feature_names_out()
imp = pd.Series(rf.named_steps["clf"].feature_importances_, index=names).sort_values()
print(imp.tail(15).round(4))

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    imp.tail(15).plot.barh(figsize=(8, 6), color="#7c5cff")
    plt.title("Топ-15 признаков — риск просрочки доставки")
    plt.tight_layout()
    out = os.path.join(os.path.dirname(__file__), "late_delivery_importances.png")
    plt.savefig(out, dpi=130)
    print("saved", out)
except Exception as e:
    print("график пропущен:", e)

# %% [markdown]
# ## Итоги
# Ключевые признаки риска просрочки (фрахт, штат) и сравнение ROC-AUC моделей — в выводе ячеек выше.
