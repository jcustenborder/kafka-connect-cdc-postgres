INSERT INTO money_table (value) values (100.52);
INSERT INTO money_table (value) values (100.52);
UPDATE money_table SET VALUE = 1002345.52 WHERE ID = 2;
UPDATE money_table SET VALUE = null WHERE ID = 2;
DELETE FROM money_table WHERE ID = 2;