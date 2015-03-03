USE analytics
go
 
SELECT
    *
FROM (
    -- select three: sequence and number; we only need the rows with seq=1, which is the last day day of each month, since we are
    --looking for the weight of federal, prov, muni, corporate and other in the last day of each month
    SELECT
        tbl.*,
        row_number() OVER(     --in each row, give a number, this is done by partition
            PARTITION BY month(tbl.date), year(tbl.date)
            ORDER BY tbl.date DESC
        ) seq
    FROM (
        -- select two: pivot
        SELECT
            date,
            ISNULL(federal, 0) as federal, ISNULL(prov, 0) as prov, ISNULL(muni, 0)as muni, ISNULL(corporate, 0) as corporate, ISNULL(other, 0) AS other
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
                AND ss.hierarchy = 'DEX'    --put this condition here, since the value returned
            --doesnt make sense if put it as a condition
            WHERE
                sp.account_code = '1031'
                AND sh.[update] BETWEEN '20060315' AND '20131015'  --we can also do
            --sh.[update] > '20060315' and sh.[update] < '20131015'
            GROUP BY sh.[update], sp.account_code,   --when use group by, put all items from select statement here
                (CASE
                    WHEN ss.level_1 = 'government' and ss.level_2 = 'federal' then 'federal'
                    WHEN ss.level_1 = 'government' and ss.level_2 = 'provincial' then 'prov'
                    WHEN ss.level_1 = 'government' and ss.level_2 = 'municipal' then 'muni'
                    WHEN ss.level_1 = 'corporate' THEN 'corporate'
                    ELSE 'other'
                END)
        ) sect
        PIVOT (MAX([weight_mv]) FOR sector in ([federal], [prov], [muni], [corporate], [other])) as pvt
        --pivot in sql is the same as in the excel
    ) tbl
) final
WHERE
    seq = 1
    AND month(date) in (3,6,9,12)  --we are only looking for these four months
ORDER BY date
