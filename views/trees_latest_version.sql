ELECT * EXCEPT(row_num)
  FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at desc) AS row_num
          FROM `strange-terra-273917.geodata_clean.trees` trees
        )
where row_num = 1