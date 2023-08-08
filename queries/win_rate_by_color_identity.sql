SELECT
    color_identity,
    SUM(wins)/SUM(games) AS winrate
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
GROUP BY color_identity
ORDER BY winrate DESC