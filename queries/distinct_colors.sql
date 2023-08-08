SELECT DISTINCT color
FROM card_cost
WHERE color NOT LIKE 'Multicolor'
   AND color NOT LIKE 'X'
ORDER BY color ASC