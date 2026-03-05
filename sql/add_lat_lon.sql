-- Ajout des colonnes de géolocalisation dans valeursfoncieres

ALTER TABLE valeursfoncieres
  ADD COLUMN latitude  DOUBLE NULL AFTER surface_terrain,
  ADD COLUMN longitude DOUBLE NULL AFTER latitude;

ALTER TABLE valeursfoncieres
  ADD INDEX idx_vf_lat_lon (latitude, longitude);

