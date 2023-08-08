WITH
    title_agg AS (
    SELECT
        color_identity,
        SUM(wins) AS wins,
        SUM(games) AS games
    FROM {{#6-5-final-model}} AS step_5
    WHERE TRUE
      AND types NOT LIKE '%Land%'
      [[AND games >= {{games}}]]
      [[AND tier = {{tier}}]]
      [[AND set_id = {{set}}]]
      [[AND rarity = {{rarity}}]]
      [[AND cmc = {{cmc}}]]
      [[AND is_legendary = {{legendary}}]]
      [[AND types LIKE CONCAT('%', {{type}}, '%')]]
      GROUP BY title)
  
SELECT
    color_identity AS 'Color identity',
    SUM(wins)/SUM(games) AS 'Win rate',
    SUM(games) AS Popularity,
    COUNT(*) AS 'Distinct titles'
FROM title_agg
GROUP BY color_identity
ORDER BY SUM(games) DESC