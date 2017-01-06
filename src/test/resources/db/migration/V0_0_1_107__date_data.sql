INSERT INTO interval_table (value) VALUES (INTERVAL '1483681887 SECONDS');
INSERT INTO interval_table (value) VALUES (INTERVAL '103 YEARS 7 MONTH 8 DAYS 4 HOURS 3 MINUTES 29 SECONDS');
INSERT INTO timestamp_table (value) VALUES (TIMESTAMP '2001-09-28 01:00:00');
INSERT INTO date_table (value) VALUES (DATE '2001-10-05');
INSERT INTO timestamp_wtz_table (value) VALUES (make_timestamptz(2013, 7, 15, 8, 15, 23.5));
INSERT INTO timestamp_wotz_table (value) VALUES (make_timestamp(2013, 7, 15, 8, 15, 23.5));
INSERT INTO time_table (value) VALUES (TIME '04:00:00');
INSERT INTO time_wtz_table (value) VALUES (TIME '04:00:00' AT TIME ZONE 'MST');
INSERT INTO time_wotz_table (value) VALUES (TIME WITH TIME ZONE '04:00:00-05');