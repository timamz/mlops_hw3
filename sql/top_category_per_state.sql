WITH state_category_totals AS
(
    SELECT
        us_state,
        cat_id,
        sum(amount) AS total_amount
    FROM transactions
    GROUP BY us_state, cat_id
)
SELECT
    us_state,
    cat_id AS top_category,
    total_amount
FROM
(
    SELECT
        us_state,
        cat_id,
        total_amount,
        row_number() OVER (PARTITION BY us_state ORDER BY total_amount DESC) AS rn
    FROM state_category_totals
)
WHERE rn = 1
ORDER BY us_state;
