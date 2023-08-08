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
          [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
          [[AND color_identity LIKE {{color_identity}}]]
          [[AND types LIKE CONCAT('%', {{type}})]]
          [[AND UPPER(title) LIKE UPPER(CONCAT('%', {{title}}, '%'))]] 
        GROUP BY id),
        
    histogram AS (
        SELECT 
            FLOOR(winrate*20)/20 AS bins,
            COUNT(winrate) AS count
        FROM wr_table
        GROUP BY bins)

SELECT * FROM histogram