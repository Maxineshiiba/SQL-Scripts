---------------written in MS SQL Server

USE analytics
go
-- it is the same code as the previous one, just switch the order of PIVOT and PARTITION
SELECT
    date,
    ISNULL(federal, 0) as federal, ISNULL(prov, 0) as prov, ISNULL(muni, 0)as muni, ISNULL(corporate, 0) as corporate, ISNULL(other, 0) AS other
FROM
(
SELECT
    *
FROM (
    -- select three: sequence and number
    SELECT
        tbl.*,
        row_number() OVER(
            PARTITION BY month(tbl.date), year(tbl.date)
            ORDER BY tbl.date DESC
        ) seq
    FROM (
        -- select one: GROUP: most imp frequently used
        SELECT
            sh.[update] as date, sp.account_code, SUM(ISNULL(sh.weight_mv, 0)) as weight_mv,
            (CASE
                WHEN ss.level_1 = 'government' and ss.level_2 = 'federal' then 'federal'
                WHEN ss.level_1 = 'government' and ss.level_2 = 'provincial' then 'prov'
                WHEN ss.level_1 = 'government' and ss.level_2 = 'municipal' then 'muni'
                WHEN ss.level_1 = 'corporate' THEN 'corporate'
                ELSE 'other'
            END) as sector
        FROM
            s_portfolio sp
            INNER JOIN s_holdings sh
            ON sp.port_id = sh.port_id
 
            LEFT JOIN s_security_static sss
            ON sh.security_id = sss.security_id
 
            LEFT JOIN s_sector ss
            ON ss.id = sss.hierarchy_dex
            AND ss.hierarchy = 'DEX'    --cant be added at the end, since something missing after
        WHERE
            sp.account_code = '1031'
            AND sh.[update] BETWEEN '20060315' AND '20131015'
        GROUP BY sh.[update], sp.account_code,
            (CASE
                WHEN ss.level_1 = 'government' and ss.level_2 = 'federal' then 'federal'
                WHEN ss.level_1 = 'government' and ss.level_2 = 'provincial' then 'prov'
                WHEN ss.level_1 = 'government' and ss.level_2 = 'municipal' then 'muni'
                WHEN ss.level_1 = 'corporate' THEN 'corporate'
                ELSE 'other'
            END)
        ) tbl
) tbl
WHERE
    seq = 1
    AND month(date) in (3,6,9,12)
) piv
PIVOT (MAX([weight_mv]) FOR sector in ([federal], [prov], [muni], [corporate], [other])) as pvt
ORDER BY date
