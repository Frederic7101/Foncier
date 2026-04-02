/**
 * Résolution des clés colonnes API pour la rentabilité (types DVF, tranches S/T).
 */
import { S } from "./comparaison_state.js";

export function rentaKeyPairForTypeLogt(typeCode) {
  var u = String(typeCode || "TOUS").toUpperCase();
  if (u === "PARKING") return { rb: "renta_brute_parking", rn: "renta_nette_parking" };
  if (u === "LOCAL_INDUS") return { rb: "renta_brute_local_indus", rn: "renta_nette_local_indus" };
  if (u === "TERRAIN") return { rb: "renta_brute_terrain", rn: "renta_nette_terrain" };
  if (u === "IMMEUBLE") return { rb: "renta_brute_immeuble", rn: "renta_nette_immeuble" };
  return null;
}

export function isRentabiliteTypeSupported(typeCode) {
  var u = String(typeCode || "TOUS").toUpperCase();
  if (!u || u === "TOUS") return true;
  if (u === "MAISON" || u === "APPART") return true;
  return rentaKeyPairForTypeLogt(u) != null;
}

export function resolveTypeScoreKey(scorePrincipal, typeCode, cat) {
  if (cat !== "rentabilite") return scorePrincipal;
  var t = String(typeCode || "TOUS").toUpperCase();
  if (!t || t === "TOUS") return scorePrincipal;
  if (t === "MAISON") {
    if (scorePrincipal === "renta_brute") return "renta_brute_maisons";
    if (scorePrincipal === "renta_nette") return "renta_nette_maisons";
    return scorePrincipal;
  }
  if (t === "APPART") {
    if (scorePrincipal === "renta_brute") return "renta_brute_appts";
    if (scorePrincipal === "renta_nette") return "renta_nette_appts";
    return scorePrincipal;
  }
  var rp = rentaKeyPairForTypeLogt(t);
  if (rp) {
    if (scorePrincipal === "renta_brute") return rp.rb;
    if (scorePrincipal === "renta_nette") return rp.rn;
    return scorePrincipal;
  }
  if (!isRentabiliteTypeSupported(typeCode)) return S.RENTA_UNAVAILABLE_KEY;
  return scorePrincipal;
}

/**
 * Colonne API pour rentabilité selon type + tranche surface (S1–S5) ou pièces (T1–T5).
 * Priorité surface si les deux sont renseignés (aligné vf_communes / backend).
 * Pas de repli sur l’agrégat sans tranche : clés dédiées ou indisponible.
 */
export function resolveFullRentabiliteScoreKey(scorePrincipal, typeLogt, typeSurf, nbPieces, cat) {
  if (cat !== "rentabilite") return scorePrincipal;
  var sp = String(scorePrincipal || "renta_nette");
  var isBrute = sp.indexOf("renta_brute") === 0;
  var pref = isBrute ? "renta_brute" : "renta_nette";
  var t = String(typeLogt || "TOUS").toUpperCase();
  var surf = String(typeSurf || "TOUTES").toUpperCase();
  var pie = String(nbPieces || "TOUS").toUpperCase();
  var hasSurf = surf !== "TOUTES" && surf !== "" && /^S[1-5]$/.test(surf);
  var hasPie = pie !== "TOUS" && pie !== "" && /^T[1-5]$/.test(pie);
  if (hasSurf) {
    if (t === "MAISON") return pref + "_maisons_" + surf.toLowerCase();
    if (t === "APPART") return pref + "_appts_" + surf.toLowerCase();
    if (!t || t === "TOUS") return pref + "_agg_" + surf.toLowerCase();
    return S.RENTA_UNAVAILABLE_KEY;
  }
  if (hasPie) {
    if (t === "MAISON") return pref + "_maisons_" + pie.toLowerCase();
    if (t === "APPART") return pref + "_appts_" + pie.toLowerCase();
    if (!t || t === "TOUS") return pref + "_agg_" + pie.toLowerCase();
    return S.RENTA_UNAVAILABLE_KEY;
  }
  return resolveTypeScoreKey(scorePrincipal, typeLogt, cat);
}
