INSERT INTO varchar_table (value) VALUES ('this is a text value');
INSERT INTO varchar_table (value) VALUES ('this is a text value');
UPDATE varchar_table SET VALUE = 'this is a text another value' WHERE ID = 2;
UPDATE varchar_table SET VALUE = null WHERE ID = 2;
DELETE FROM varchar_table WHERE ID = 2;

INSERT INTO character_varying_table (value) VALUES ('this is a text value');
INSERT INTO character_varying_table (value) VALUES ('this is a text value');
UPDATE character_varying_table SET VALUE = 'this is a text another value' WHERE ID = 2;
UPDATE character_varying_table SET VALUE = null WHERE ID = 2;
DELETE FROM character_varying_table WHERE ID = 2;

INSERT INTO char_table (value) VALUES ('this is a text value');
INSERT INTO char_table (value) VALUES ('this is a text value');
UPDATE char_table SET VALUE = 'this is a text another value' WHERE ID = 2;
UPDATE char_table SET VALUE = null WHERE ID = 2;
DELETE FROM char_table WHERE ID = 2;

INSERT INTO character_table (value) VALUES ('this is a text value');
INSERT INTO character_table (value) VALUES ('this is a text value');
UPDATE character_table SET VALUE = 'this is a text another value' WHERE ID = 2;
UPDATE character_table SET VALUE = null WHERE ID = 2;
DELETE FROM character_table WHERE ID = 2;

INSERT INTO text_table (value) VALUES ('this is a text value');
INSERT INTO text_table (value) VALUES ('this is a text value');
UPDATE text_table SET VALUE = 'this is a text another value' WHERE ID = 2;
UPDATE text_table SET VALUE = null WHERE ID = 2;
DELETE FROM text_table WHERE ID = 2;


INSERT INTO bit_table (value) VALUES (B'101');
INSERT INTO bit_table (value) VALUES (B'101');
UPDATE bit_table SET VALUE = B'000' WHERE ID = 2;
UPDATE bit_table SET VALUE = null WHERE ID = 2;
DELETE FROM bit_table WHERE ID = 2;

INSERT INTO bit_varying_table (value) VALUES (B'101');
INSERT INTO bit_varying_table (value) VALUES (B'101');
UPDATE bit_varying_table SET VALUE = B'000' WHERE ID = 2;
UPDATE bit_varying_table SET VALUE = null WHERE ID = 2;
DELETE FROM bit_varying_table WHERE ID = 2;

INSERT INTO uuid_table (value) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
INSERT INTO uuid_table (value) VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
UPDATE uuid_table SET VALUE = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd381a11' WHERE ID = 2;
UPDATE uuid_table SET VALUE = null WHERE ID = 2;
DELETE FROM uuid_table WHERE ID = 2;

INSERT INTO json_table (value) VALUES ('{"foo":"bar"}');
INSERT INTO json_table (value) VALUES ('{"foo":"bar"}');
UPDATE json_table SET VALUE = '{"foo":"foo"}' WHERE ID = 2;
UPDATE json_table SET VALUE = null WHERE ID = 2;
DELETE FROM json_table WHERE ID = 2;

INSERT INTO jsonb_table (value) VALUES ('{"foo":"bar"}');
INSERT INTO jsonb_table (value) VALUES ('{"foo":"bar"}');
UPDATE jsonb_table SET VALUE = '{"foo":"foo"}' WHERE ID = 2;
UPDATE jsonb_table SET VALUE = null WHERE ID = 2;
DELETE FROM jsonb_table WHERE ID = 2;

INSERT INTO pg_lsn_table (value) VALUES ('16/B374D848');
INSERT INTO pg_lsn_table (value) VALUES ('16/B374D848');
UPDATE pg_lsn_table SET VALUE = '17/B374D848' WHERE ID = 2;
UPDATE pg_lsn_table SET VALUE = null WHERE ID = 2;
DELETE FROM pg_lsn_table WHERE ID = 2;

INSERT INTO xml_table (value) VALUES ('<root/>');
INSERT INTO xml_table (value) VALUES ('<root/>');
UPDATE xml_table SET VALUE = '<notroot/>' WHERE ID = 2;
UPDATE xml_table SET VALUE = null WHERE ID = 2;
DELETE FROM xml_table WHERE ID = 2;


INSERT INTO varchar_table (value) VALUES (null);
INSERT INTO character_varying_table (value) VALUES (null);
INSERT INTO char_table (value) VALUES (null);
INSERT INTO character_table (value) VALUES (null);
INSERT INTO text_table (value) VALUES (null);
INSERT INTO bit_table (value) VALUES (B'101');
INSERT INTO bit_varying_table (value) VALUES (B'101');
INSERT INTO uuid_table (value) VALUES (null);
INSERT INTO json_table (value) VALUES (null);
INSERT INTO jsonb_table (value) VALUES (null);
INSERT INTO pg_lsn_table (value) VALUES (null);
INSERT INTO xml_table (value) VALUES (null);
