CREATE TABLE IF NOT EXISTS Stanowiska_pomiarowe (

  id INTEGER PRIMARY KEY,
  id_stacji INTEGER NOT NULL,

  wskaznik TEXT NOT NULL,
  wskaznik_wzor TEXT NOT NULL,
  wskaznik_kod TEXT NOT NULL,

  id_wskaznik INTEGER NOT NULL,

  FOREIGN KEY (id_stacji) REFERENCES Stacje(id)
);
