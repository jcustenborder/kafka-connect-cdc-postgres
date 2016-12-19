CREATE TABLE SIMPLE_TABLE (
  user_id BIGSERIAL PRIMARY KEY,
  email_address VARCHAR(512),
  first_name VARCHAR(256),
  last_name VARCHAR(256),
  description TEXT
);