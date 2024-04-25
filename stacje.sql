CREATE TABLE IF NOT EXISTS Stacje (
  id INTEGER PRIMARY KEY,
  nazwa_stacji TEXT NOT NULL,
  wojewodztwo TEXT NOT NULL,
  latitude REAL,
  longitude REAL
);

