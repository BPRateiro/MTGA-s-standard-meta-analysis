WITH 
    wr_table AS(
        SELECT
            SUM(wins)/SUM(games) AS winrate
        FROM {{#6-5-final-model}} AS step_5
        WHERE TRUE
          [[AND games >= {{games}}]]
          [[AND tier = {{tier}}]]
          [[AND set_id = {{set}}]]
          [[AND rarity = {{rarity}}]]
          [[AND cmc = {{cmc}}]]
          [[AND is_legendary = {{legendary}}]]
          [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
          [[AND color_identity LIKE {{color_identity}}]]
          [[AND types LIKE CONCAT('%', {{type}}, '%')]]
          [[AND UPPER(title) LIKE UPPER(CONCAT('%', {{title}}, '%'))]] 
        GROUP BY id)
        
SELECT 
    CEIL(winrate*50)/50 AS bins,
    COUNT(winrate) AS count
FROM wr_table
GROUP BY bins
    HAVING bins >= 0.5