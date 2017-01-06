INSERT INTO bytea_table (value) VALUES (E'\\xDEADBEEF');
INSERT INTO bytea_table (value) VALUES (E'\\xDEADBEEF');
UPDATE bytea_table SET VALUE = E'\\xDEADBEEFDEADBEEF' WHERE ID = 2;
DELETE FROM bytea_table WHERE ID = 2;