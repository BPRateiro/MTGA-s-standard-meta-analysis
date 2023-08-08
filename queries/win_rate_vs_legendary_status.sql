WITH
    wr_table AS(
        SELECT
            is_legendary,
            SUM(wins)/SUM(games) AS winrate
        FROM {{#6-5-final-model}} AS step_5
        WHERE TRUE
          [[AND games >= {{games}}]]
          [[AND tier = {{tier}}]]
          [[AND set_id = {{set}}]]
          [[AND cmc = {{cmc}}]]
          [[AND types LIKE CONCAT('%', {{type}}, '%')]]
          [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
          [[AND color_identity LIKE {{color_identity}}]]
          [[AND rarity = {{rarity}}]]
        GROUP BY id)

SELECT
    is_legendary,
    AVG(winrate) AS winrate,
    COUNT(*) AS distinct_titles
FROM wr_table
GROUP BY is_legendary
    [[HAVING is_legendary = {{legendary}}]]
ORDER By winrate DESC