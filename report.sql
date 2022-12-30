CREATE DATABASE training;

CREATE TABLE IF NOT EXISTS training.action_vimp
(
    `campaign_id` Int64,
    `user_id` Int64,
    `timestamp` Int64
)
ENGINE = MergeTree()
ORDER BY timestamp;

INSERT INTO training.action_vimp (`campaign_id`, `user_id`, `timestamp`) VALUES (1,1,toUnixTimestamp(now()));

CREATE TABLE IF NOT EXISTS training.action_click
(
    `ots` String,
    `campaign_id` Int64,
    `user_id` Int64,
    `timestamp` Int64
)
ENGINE = MergeTree()
ORDER BY timestamp;

INSERT INTO training.action_click (`campaign_id`, `user_id`, `timestamp`) VALUES (1,1,toUnixTimestamp(now()));


CREATE TABLE IF NOT EXISTS training.report
(
    `timestamp_hour` Int64,
    `campaign_id` Int64,
    `vimp` Int64,
    `click` Int64
)
ENGINE = MergeTree()
ORDER BY (timestamp_hour);

CREATE MATERIALIZED VIEW training.report_vimp_mv TO training.report
(
    `timestamp_hour` Int64,
    `campaign_id` Int64,
    `vimp` Int64,
    `click` Int64
) AS
SELECT
    timestamp - (timestamp % 3600) AS timestamp_hour,
    campaign_id,
    count(*) AS vimp,
    0 AS click
FROM training.action_vimp
GROUP BY
    timestamp_hour,
    campaign_id,
    click;

CREATE MATERIALIZED VIEW training.report_click_mv TO training.report
(
    `timestamp_hour` Int64,
    `campaign_id` Int64,
    `vimp` Int64,
    `click` Int64
) AS
SELECT
    timestamp - (timestamp % 3600) AS timestamp_hour,
    campaign_id,
    0 AS vimp,
    count(*) AS click
FROM training.action_click
GROUP BY
    timestamp_hour,
    campaign_id,
    vimp;
