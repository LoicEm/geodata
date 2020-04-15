SELECT trees.*
  FROM `strange-terra-273917.snapshot.trees` trees,
       `strange-terra-273917.snapshot.buildings` buildings
WHERE NOT ST_CONTAINS(buildings.geometry, trees.geometry)