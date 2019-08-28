
With CTE_PointsAndNodes as 
(
SELECT gid
,statefp
,countyfp
,tractce
,geoid

, (st_dumppoints(geom)).geom as points  
, (st_dumppoints(geom)).path as path         
FROM (Select * from acs_2016_block_groups) as tblShapefile
  )
  

 Select gid  
,statefp
,countyfp
,tractce
,geoid
--  , geom
--  , points
--  , Array_to_string(path,',') as Path_Str
, Split_Part(Array_to_string(path,','),',',1)::integer as tblu_id_1 
, Split_Part(Array_to_string(path,','),',',2)::integer as tblu_id_2 
, Split_Part(Array_to_string(path,','),',',3)::integer as tblu_path 
, st_x(st_transform(points,4326)) as x_axis 
, st_y(st_transform(points,4326)) as y_axis
INTO acs_2016_block_groups_tblu                   
From CTE_PointsAndNodes