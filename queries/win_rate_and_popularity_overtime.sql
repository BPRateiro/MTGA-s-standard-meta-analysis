SELECT
    SUM(ag.wins)/SUM(ag.games) AS winrate,
    SUM(ag.games)/SUM(d.total) AS popularity,
    DATE(ag.created_on) AS 'date'
FROM analytics_games AS ag
LEFT JOIN card AS c
    ON ag.card_id = c.id
LEFT JOIN distinct_games AS d
    ON ag.distinct_id = d.id 
WHERE c.art_link = {{art}}
    [[AND ag.tier = {{tier}}]]
GROUP BY c.id, DATE(ag.created_on)