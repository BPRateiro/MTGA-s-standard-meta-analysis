SELECT
    id,
    art_link,
    title,
    set_id,
    rarity,
    is_legendary,
    COALESCE(cmc, 0) AS cmc,
    types,
    colors,
    color_identity,
    SUM(wins)/SUM(games) AS winrate,
    SUM(games)/SUM(step_5.unique) AS popularity,
    SUM(games) AS games,
    CASE greatest(SUM(copies_1), SUM(copies_2), SUM(copies_3), SUM(copies_4))
        WHEN SUM(copies_1) THEN 1
        WHEN SUM(copies_2) THEN 2
        WHEN SUM(copies_3) THEN 3
        WHEN SUM(copies_4) THEN 4
    END AS copies
FROM {{#6-5-final-model}} AS step_5
WHERE TRUE
  [[AND games >= {{games}}]]
  [[AND tier = {{tier}}]]
  [[AND set_id = {{set}}]]
  [[AND rarity = {{rarity}}]]
  [[AND is_legendary = {{legendary}}]]
  [[AND colors LIKE CONCAT('%', {{color}}, '%')]]
  [[AND color_identity LIKE {{color_identity}}]]
  [[AND types LIKE CONCAT('%', {{type}}, '%')]]
  [[AND cmc = {{cmc}}]]
  [[AND UPPER(title) LIKE UPPER(CONCAT('%', {{title}}, '%'))]] 
GROUP BY id
ORDER BY popularity DESC