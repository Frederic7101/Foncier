-- Correction des types INTEGER → BIGINT pour éviter "integer out of range"
-- sur montant_adjudication et mise_a_prix (propriétés très chères)

ALTER TABLE foncier.licitor_brut
    ALTER COLUMN montant_adjudication SET DATA TYPE BIGINT,
    ALTER COLUMN mise_a_prix SET DATA TYPE BIGINT;
