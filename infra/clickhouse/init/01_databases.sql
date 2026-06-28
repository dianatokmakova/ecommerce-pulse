-- Слои медальон-архитектуры
CREATE DATABASE IF NOT EXISTS bronze;   -- сырьё «как пришло»
CREATE DATABASE IF NOT EXISTS silver;   -- очищенное, типизированное
CREATE DATABASE IF NOT EXISTS gold;     -- витрины под дашборды

-- read-only пользователь для BI (DataLens / dashboard-api): доступ только на gold/silver, без DROP/INSERT.
CREATE USER IF NOT EXISTS bi_reader IDENTIFIED BY 'change_me_bi_password';
GRANT SELECT ON gold.*   TO bi_reader;
GRANT SELECT ON silver.* TO bi_reader;
