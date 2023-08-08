SELECT
    step_4.*,
    step_3.tier,
    step_3.unique,
    step_3.games,
    step_3.wins,
    step_3.copies_1,
    step_3.copies_2,
    step_3.copies_3,
    step_3.copies_4
FROM (SELECT
    step_2.card_id,
    step_2.tier,
    step_2.unique,
    step_2.games,
    step_2.wins,
    COALESCE(ad_1.played, 0) AS copies_1,
    COALESCE(ad_2.played, 0) AS copies_2,
    COALESCE(ad_3.played, 0) AS copies_3,
    COALESCE(ad_4.played, 0) AS copies_4
FROM (SELECT
    ag.id,
    ag.card_id,
    ag.tier,
    dg.total AS 'unique',
    ag.games,
    ag.wins
FROM (SELECT
    id,
    card_id,
    tier,
    distinct_id,
    games,
    wins
FROM analytics_games
WHERE created_on IN (
    SELECT MAX(created_on)
    FROM analytics_games
)) AS ag
LEFT JOIN distinct_games AS dg
    ON ag.distinct_id = dg.id) AS step_2
LEFT JOIN analytics_distribution AS ad_1
    ON step_2.id = ad_1.games_id
   
   AND ad_1.copies = 1
LEFT JOIN analytics_distribution AS ad_2
    ON step_2.id = ad_2.games_id
    AND ad_2.copies = 2
LEFT JOIN analytics_distribution AS ad_3
    ON step_2.id = ad_3.games_id
    AND ad_3.copies = 3
LEFT JOIN analytics_distribution AS ad_4
    ON step_2.id = ad_4.games_id
    AND ad_4.copies = 4) AS step_3
LEFT JOIN (WITH 
    filtered_cost AS(
        SELECT
            card_id,
            COALESCE(SUM(cost), 0) AS cmc,
            GROUP_CONCAT(color) AS colors
        FROM card_cost
        WHERE color <> 'X'
       
GROUP BY card_id
       
ORDER BY cmc),
        
    filtered_types AS(
        SELECT
            card_id,
            GROUP_CONCAT(type) AS types
        FROM card_type
        GROUP BY card_id
    )

SELECT
    card.id,
    card.art_link,
    card.set_id,
    card.title,
    card.rarity,
    card.power,
    card.toughness,
    CASE 
        WHEN card.is_legendary = 1 THEN 'Yes'
        ELSE 'No'
    END AS is_legendary,
    COALESCE(fc.cmc, 0) AS cmc,
    fc.colors,
    CASE
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Rainbow'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Dune'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            THEN 'Glint'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Ink'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%White%'
            THEN 'Witch'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Yore'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%White%'
            THEN 'Abzan'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%White%'
            THEN 'Bant'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%White%'
            THEN 'Esper'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Red%'
            THEN 'Grixis'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Jeskai'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            THEN 'Jund'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Mardu'
        WHEN TRUE
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Naya'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            THEN 'Sultai'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            THEN 'Temur'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%White%'
            THEN 'Azorius'
        WHEN TRUE
            AND fc.colors LIKE '%Red%'
            AND fc.colors LIKE '%White%'
            THEN 'Boros'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Blue%'
            THEN 'Dimir'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Green%'
            THEN 'Golgari'
        WHEN TRUE
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%Red%'
            THEN 'Gruul'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Red%'
            THEN 'Izzet'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%White%'
            THEN 'Orzhov'
        WHEN TRUE
            AND fc.colors LIKE '%Black%'
            AND fc.colors LIKE '%Red%'
            THEN 'Rakdos'
        WHEN TRUE
            AND fc.colors LIKE '%Green%'
            AND fc.colors LIKE '%White%'
            THEN 'Selesnya'
        WHEN TRUE
            AND fc.colors LIKE '%Blue%'
            AND fc.colors LIKE '%Green%'
            THEN 'Simic'
        WHEN fc.colors LIKE '%Black%' THEN 'Black'
        WHEN fc.colors LIKE '%Blue%' THEN 'Blue'
        WHEN fc.colors LIKE '%Green%' THEN 'Green'
        WHEN fc.colors LIKE '%Red%' THEN 'Red'
        WHEN fc.colors LIKE '%White%' THEN 'White'
        ELSE 'Colorless'
        END AS color_identity,
    ft.types
FROM card
LEFT JOIN filtered_cost AS fc
    ON fc.card_id = card.id
LEFT JOIN filtered_types AS ft
    ON ft.card_id = card.id) AS step_4
    ON step_3.card_id = step_4.id