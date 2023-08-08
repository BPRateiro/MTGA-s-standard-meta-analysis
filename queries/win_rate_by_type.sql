WITH
    wr_table AS(
        SELECT
            types,
            SUM(wins)/SUM(games) AS winrate
        FROM {{#6-5-final-model}} AS step_5
        WHERE TRUE
          [[AND games >= {{games}}]]
          [[AND tier = {{tier}}]]
          [[AND set_id = {{set}}]]
          [[AND rarity = {{rarity}}]]
          [[AND is_legendary = {{legendary}}]]
          [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
          [[AND color_identity LIKE {{color_identity}}]]
          [[AND cmc = {{cmc}}]]
        GROUP BY id)

SELECT
    type,
    AVG(winrate) AS winrate,
    COUNT(*) AS distinct_titles
FROM wr_table
LEFT JOIN {{#8-distinct-types}} AS distinct_types
    ON wr_table.types LIKE CONCAT('%', distinct_types.type, '%')
GROUP BY type
    [[HAVING type LIKE CONCAT('%', {{type}}, '%')]]
ORDER By winrate DESC