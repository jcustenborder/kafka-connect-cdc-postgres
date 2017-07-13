CREATE TABLE "users" (
  id             SERIAL PRIMARY KEY,
  first_name     VARCHAR(255) DEFAULT NULL,
  last_name      VARCHAR(255) DEFAULT NULL,
  email_address  VARCHAR(255) DEFAULT NULL,
  street_address VARCHAR(255) DEFAULT NULL,
  city           VARCHAR(255),
  region         VARCHAR(50)  DEFAULT NULL,
  country        VARCHAR(100) DEFAULT NULL,
  company        VARCHAR(255)
);