INSERT into point_table (value) VALUES (point(30.2672, 97.7431));
INSERT into point_table (value) VALUES (point(30.2672, 97.7431));
UPDATE point_table SET VALUE = point(-30.2672, 97.7431) WHERE ID = 2;
UPDATE point_table SET VALUE = null WHERE ID = 2;
DELETE FROM point_table WHERE ID = 2;

INSERT into box_table (value) VALUES (box(point '(0,0)', point '(1,1)'));
INSERT into box_table (value) VALUES (box(point '(0,0)', point '(1,1)'));
UPDATE box_table SET VALUE = (box(point '(0,0)', point '(2,2)')) WHERE ID = 2;
UPDATE box_table SET VALUE = null WHERE ID = 2;
DELETE FROM box_table WHERE ID = 2;

INSERT into circle_table (value) VALUES (circle(point '(0,0)', 2.0));
INSERT into circle_table (value) VALUES (circle(point '(0,0)', 2.0));
UPDATE circle_table SET VALUE = (circle(point '(0,0)', 3.0)) WHERE ID = 2;
UPDATE circle_table SET VALUE = null WHERE ID = 2;
DELETE FROM circle_table WHERE ID = 2;

INSERT into polygon_table (value) VALUES (polygon(path '((0,0),(1,1),(2,0))'));
INSERT into path_table (value) VALUES (path('((0,0),(1,1),(2,0))'));
INSERT into lseg_table (value) VALUES (lseg(box('((-1,0),(1,0))')));

-- Seattle
-- point(47.6062, 122.3321)
-- Los Angeles
-- point(34.0522, 118.2437)
-- New York
-- point(40.7128, 74.0059)
-- Miami
-- point(25.7617, 80.1918)

-- INSERT into line_table (value) VALUES (line(30.2672, 97.7431));


--
--
-- CREATE TABLE line_table (
--   ID    BIGSERIAL PRIMARY KEY NOT NULL,
--   value LINE
-- );
--



