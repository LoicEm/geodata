with subrequest as (
  SELECT
        buildings.id as building_id,
        buildings.geometry as building_geometry,
        trees.geometry as tree_geometry,
        trees.id as tree_id,
        ST_DISTANCE(buildings.geometry, trees.geometry) as tree_distance,
        COUNT(*) OVER (PARTITION BY buildings.id) AS n_trees_within_100_meters,
        ROW_NUMBER() OVER (PARTITION BY buildings.id
                           ORDER BY ST_DISTANCE(buildings.geometry, trees.geometry)) AS rk
      FROM geodata_clean.buildings, geodata_clean.trees
      WHERE ST_DISTANCE(buildings.geometry, trees.geometry) < 100)
SELECT building_id, tree_id as closest_tree_id, n_trees_within_100_meters
FROM subrequest
where rk = 1