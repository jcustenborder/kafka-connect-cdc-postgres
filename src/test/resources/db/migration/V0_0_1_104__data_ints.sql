INSERT INTO smallint_table (value) VALUES (1234);
INSERT INTO smallint_table (value) VALUES (1234);
UPDATE smallint_table SET VALUE = 4321 WHERE ID = 2;
UPDATE smallint_table SET VALUE = null WHERE ID = 2;
DELETE FROM smallint_table WHERE ID = 2;

INSERT INTO integer_table (value) VALUES (1234);
INSERT INTO integer_table (value) VALUES (1234);
UPDATE integer_table SET VALUE = 4321 WHERE ID = 2;
UPDATE integer_table SET VALUE = null WHERE ID = 2;
DELETE FROM integer_table WHERE ID = 2;

INSERT INTO bigint_table (value) VALUES (1234);
INSERT INTO bigint_table (value) VALUES (1234);
UPDATE bigint_table SET VALUE = 4321 WHERE ID = 2;
UPDATE bigint_table SET VALUE = null WHERE ID = 2;
DELETE FROM bigint_table WHERE ID = 2;

INSERT INTO serial_table (value) VALUES ('testing');
INSERT INTO smallserial_table (value) VALUES ('testing');
INSERT INTO boolean_table (value) VALUES (true);
