WITH
    wr_table AS(
        SELECT
            cmc,
            SUM(wins)/SUM(games) AS winrate
        FROM {{#6-5-final-model}} AS step_5
        WHERE TRUE
          [[AND games >= {{games}}]]
          [[AND tier = {{tier}}]]
          [[AND set_id = {{set}}]]
          [[AND is_legendary = {{legendary}}]]
          [[AND types LIKE CONCAT('%', {{type}}, '%')]]
          [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
          [[AND color_identity LIKE {{color_identity}}]]
          [[AND rarity = {{rarity}}]]
        GROUP BY id)

SELECT
    cmc,
    AVG(winrate) AS winrate,
    COUNT(*) AS distinct_titles
FROM wr_table
GROUP BY cmc
    [[HAVING cmc = {{cmc}}]]
ORDER By winrate DESC