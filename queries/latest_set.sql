SELECT *
FROM untapped.set
WHERE created_on IN (
    SELECT MAX(created_on)
    FROM untapped.set
)