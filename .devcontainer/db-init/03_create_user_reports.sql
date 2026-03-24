CREATE TABLE IF NOT EXISTS raw.user_reports (
  id SERIAL PRIMARY KEY,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  full_name TEXT NOT NULL,
  commune TEXT,
  location_text TEXT,
  vehicle_type TEXT,
  categorie_route TEXT,
  intersection TEXT,
  type_collision TEXT,
  luminosite TEXT,
  cond_atmos TEXT,
  gravite_usager TEXT,
  notes TEXT
);
