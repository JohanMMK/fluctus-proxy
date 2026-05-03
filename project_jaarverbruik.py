#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
project_jaarverbruik.py — Fase 2 van BaseCase Uitbreiding, sessie 2.

Single source of truth voor profielprojectie + Enwyse-staffel-tier-bepaling.
JS-port in server.js (helper `projectJaarverbruikMWh` + route POST
/api/factuur-staffel-bepalen) is 1-op-1 spiegel; logica-wijzigingen gebeuren
ALTIJD eerst hier.

Doel: gegeven een factuur-afnameKwh + factuurperiode + gekozen profiel-naam,
projecteer het verbruik naar een geprojecteerd jaarverbruik in MWh, en
bepaal in welke Enwyse-tier dat valt.

Profielen: 25 JSON-bestanden in data/profielen/<naam>.json met 35.040
kwartierwaarden (= 365 dagen × 96 kwartieren). Profielen zijn gemodelleerd op
kalenderjaar 2025 — geen schrikkeljaar, week start op woensdag (1-jan-2025 = wo).

Mapping (vraag 1B in sessie 2): WEEKDAG-BEWUST.
Voor elke datum d in [periodeVan, periodeTot]:
  - bepaal weekdag w_target (0=ma, 6=zo)
  - bepaal day-of-year doy_target (1..366)
  - zoek in 2025 een datum met dezelfde maand+dag, of bij mismatch in weekdag
    de dichtstbijzijnde datum (binnen ±3 dagen) met weekdag == w_target.
Dit voorkomt dat een 2026-zondag op een 2025-zaterdag van het profiel valt
(slager-profiel: zondag dicht, zaterdag piek — dat zou de tier kantelen).

Drempel (vraag 2): periode korter dan 14 dagen → status "ONBETROUWBAAR".

Response-shape (vraag 3, akkoord van Johan):
  ok=True:  {ok, geprojecteerdJaarverbruikMWh, tier, _diagnose}
  ok=False: {ok=False, status, reden, _diagnose}

Anti-regressie (kennisbank §4.1, regel 3): module is puur additief, leest
data/profielen/<naam>.json (READ-ONLY) en data/leveringscontract.json
(READ-ONLY). Wijzigt geen bestaande state.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

# ─── CONSTANTEN ──────────────────────────────────────────────────────────────

KWARTIEREN_PER_DAG = 96
DAGEN_2025 = 365
KWARTIEREN_2025 = DAGEN_2025 * KWARTIEREN_PER_DAG  # 35040

MIN_DAGEN_BETROUWBAAR = 14   # vraag 2: drempel voor "ONBETROUWBAAR"
WEEKDAG_ZOEK_RADIUS = 3       # ±3 dagen rondom doy om weekdag-match te vinden

# Fallback CONTRACT_STAFFEL — identiek aan server.js regels 82-96.
# In productie wordt deze uit data/leveringscontract.json geladen.
DEFAULT_CONTRACT_STAFFEL = [
    {"min_mwh": 0,    "max_mwh": 100,    "label": "0-100 MWh",    "code": "S1",
     "consumption_dam_markup": 20.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 100,  "max_mwh": 200,    "label": "100-200 MWh",  "code": "S2",
     "consumption_dam_markup": 19.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 200,  "max_mwh": 300,    "label": "200-300 MWh",  "code": "S3",
     "consumption_dam_markup": 18.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 300,  "max_mwh": 400,    "label": "300-400 MWh",  "code": "S4",
     "consumption_dam_markup": 17.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 400,  "max_mwh": 500,    "label": "400-500 MWh",  "code": "S5",
     "consumption_dam_markup": 16.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 500,  "max_mwh": 600,    "label": "500-600 MWh",  "code": "S6",
     "consumption_dam_markup": 15.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 600,  "max_mwh": 700,    "label": "600-700 MWh",  "code": "S7",
     "consumption_dam_markup": 14.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 700,  "max_mwh": 800,    "label": "700-800 MWh",  "code": "S8",
     "consumption_dam_markup": 13.5, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 800,  "max_mwh": 900,    "label": "800-900 MWh",  "code": "S9",
     "consumption_dam_markup": 13.0, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 900,  "max_mwh": 1000,   "label": "900-1000 MWh", "code": "S10",
     "consumption_dam_markup": 12.5, "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 1000, "max_mwh": 2000,   "label": "1-2 GWh",      "code": "S11",
     "consumption_dam_markup": 8.0,  "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 2000, "max_mwh": 5000,   "label": "2-5 GWh",      "code": "S12",
     "consumption_dam_markup": 5.0,  "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
    {"min_mwh": 5000, "max_mwh": 999999, "label": ">5 GWh",       "code": "S13",
     "consumption_dam_markup": 3.5,  "consumption_imbalance_markup": 5.0,
     "injection_dam_markdown": 0.0,  "injection_imbalance_markdown": 11.0},
]


# ─── HELPERS — datum & profiel-index ─────────────────────────────────────────

def _parse_iso_date(s: str) -> date:
    """Parse ISO date string YYYY-MM-DD. Tolerant voor full ISO datetime."""
    if not s:
        raise ValueError("lege datum-string")
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def _day_of_year_2025(month: int, day: int) -> int:
    """Day-of-year voor (maand, dag) in 2025 (geen schrikkeljaar).
    Returnt 1..365. Voor 29 feb (alleen schrikkeljaar) returnt 28 feb (=59).
    """
    # 2025 is geen schrikkeljaar. Voor 29 feb in 2024/2028: clamp naar 28 feb.
    if month == 2 and day == 29:
        day = 28
    return date(2025, month, day).timetuple().tm_yday


def _weekday(d: date) -> int:
    """0=ma .. 6=zo (zoals Python's date.weekday())."""
    return d.weekday()


def _profiel_kwartier_indices_voor_datum(d: date) -> list[int]:
    """Returnt 96 indices in profiel-array (lengte 35.040) voor datum d.

    Mapping is WEEKDAG-BEWUST (vraag 1B):
    - Bepaal day-of-year en weekdag van d.
    - In 2025: vind een datum dichtbij dezelfde doy met dezelfde weekdag,
      binnen ±WEEKDAG_ZOEK_RADIUS dagen. 1-jan-2025 = wo (weekdag 2).
    - Als geen exacte match binnen radius: val terug op pure doy-mapping.
    """
    target_doy = _day_of_year_2025(d.month, d.day)
    target_wd = _weekday(d)

    # Zoek dichtstbijzijnde 2025-dag met dezelfde weekdag.
    best_doy: int | None = None
    best_offset = WEEKDAG_ZOEK_RADIUS + 1
    for offset in range(-WEEKDAG_ZOEK_RADIUS, WEEKDAG_ZOEK_RADIUS + 1):
        cand_doy = target_doy + offset
        if cand_doy < 1 or cand_doy > DAGEN_2025:
            continue
        cand_date = date(2025, 1, 1) + timedelta(days=cand_doy - 1)
        if _weekday(cand_date) == target_wd and abs(offset) < best_offset:
            best_doy = cand_doy
            best_offset = abs(offset)
            if best_offset == 0:
                break

    doy = best_doy if best_doy is not None else target_doy
    base = (doy - 1) * KWARTIEREN_PER_DAG
    return [base + k for k in range(KWARTIEREN_PER_DAG)]


def _aantal_dagen(periode_van: date, periode_tot: date) -> int:
    """Aantal kalenderdagen in [van, tot] inclusief.
    Conventie identiek aan _capaciteitstariefRegels in extract.js: dag-bereik
    inclusief, dus 1-jan tot 31-jan = 31 dagen.
    """
    return (periode_tot - periode_van).days + 1


# ─── CORE — projectie & tier-keuze ───────────────────────────────────────────

def _som_profiel_in_periode(profiel_kwartier: list[float],
                             periode_van: date, periode_tot: date) -> float:
    """Som van profielwaarden voor alle kwartieren in [van, tot] inclusief,
    via weekdag-bewuste mapping op het 2025-kalender-profiel.
    """
    if len(profiel_kwartier) != KWARTIEREN_2025:
        raise ValueError(
            f"profiel_kwartier moet {KWARTIEREN_2025} waarden hebben, "
            f"kreeg er {len(profiel_kwartier)}"
        )
    totaal = 0.0
    d = periode_van
    while d <= periode_tot:
        for idx in _profiel_kwartier_indices_voor_datum(d):
            totaal += profiel_kwartier[idx]
        d += timedelta(days=1)
    return totaal


def _kies_tier(jaarverbruik_mwh: float, staffel: list[dict]) -> dict | None:
    """Kies de Enwyse-tier waarvoor min_mwh ≤ jaarverbruik < max_mwh.
    Strikte conventie (vraag 2 sessie 2: gebruiker koos "OK" = strikt zonder buffer).
    Returnt None als geen match (bv. negatief verbruik).
    """
    if jaarverbruik_mwh < 0:
        return None
    for tier in staffel:
        if tier["min_mwh"] <= jaarverbruik_mwh < tier["max_mwh"]:
            return dict(tier)
    # Boven max_mwh van laatste tier: gebruik laatste tier.
    if staffel and jaarverbruik_mwh >= staffel[-1]["min_mwh"]:
        return dict(staffel[-1])
    return None


@dataclass
class ProjectieInput:
    profiel_naam: str
    profiel_kwartier: list[float]
    afname_kwh: float
    periode_van: str   # ISO YYYY-MM-DD
    periode_tot: str   # ISO YYYY-MM-DD
    staffel: list[dict] | None = None  # default = DEFAULT_CONTRACT_STAFFEL


def project_jaarverbruik(inp: ProjectieInput) -> dict:
    """Hoofdfunctie. Returnt response-dict identiek aan wat de route stuurt.

    Mogelijk uitkomsten:
      - {ok: True, geprojecteerdJaarverbruikMWh, tier, _diagnose}
      - {ok: False, status: "ONBETROUWBAAR", reden, _diagnose}
      - {ok: False, status: "FOUT", reden, _diagnose}
    """
    staffel = inp.staffel if inp.staffel is not None else DEFAULT_CONTRACT_STAFFEL

    # Parse datums; foutmelding is een FOUT-status (snippet toont melding).
    try:
        van = _parse_iso_date(inp.periode_van)
        tot = _parse_iso_date(inp.periode_tot)
    except (ValueError, TypeError) as e:
        return {
            "ok": False, "status": "FOUT",
            "reden": f"Ongeldige datums: {e}",
            "_diagnose": {
                "profielNaam": inp.profiel_naam,
                "afnameKwh": inp.afname_kwh,
                "periodeVan": inp.periode_van,
                "periodeTot": inp.periode_tot,
            }
        }

    if tot < van:
        return {
            "ok": False, "status": "FOUT",
            "reden": f"periodeTot ({inp.periode_tot}) ligt vóór periodeVan ({inp.periode_van}).",
            "_diagnose": {
                "profielNaam": inp.profiel_naam,
                "afnameKwh": inp.afname_kwh,
                "periodeVan": inp.periode_van,
                "periodeTot": inp.periode_tot,
            }
        }

    if inp.afname_kwh is None or inp.afname_kwh <= 0:
        return {
            "ok": False, "status": "FOUT",
            "reden": "afnameKwh moet positief zijn.",
            "_diagnose": {
                "profielNaam": inp.profiel_naam,
                "afnameKwh": inp.afname_kwh,
                "periodeVan": inp.periode_van,
                "periodeTot": inp.periode_tot,
            }
        }

    dagen = _aantal_dagen(van, tot)

    # Bereken altijd de diagnose-velden, ook bij ONBETROUWBAAR — zo kan de
    # snippet ze tonen ter info en hoeft hij geen tweede call te doen.
    try:
        som_periode = _som_profiel_in_periode(inp.profiel_kwartier, van, tot)
    except ValueError as e:
        return {
            "ok": False, "status": "FOUT",
            "reden": str(e),
            "_diagnose": {
                "profielNaam": inp.profiel_naam,
                "afnameKwh": inp.afname_kwh,
                "periodeVan": inp.periode_van, "periodeTot": inp.periode_tot,
                "dagenInPeriode": dagen,
            }
        }

    som_jaar = sum(inp.profiel_kwartier)
    if som_jaar <= 0:
        return {
            "ok": False, "status": "FOUT",
            "reden": "Profiel-jaarsom is 0 of negatief — profiel-bestand corrupt.",
            "_diagnose": {
                "profielNaam": inp.profiel_naam,
                "afnameKwh": inp.afname_kwh,
                "periodeVan": inp.periode_van, "periodeTot": inp.periode_tot,
                "dagenInPeriode": dagen,
                "som_profiel_periode": som_periode,
                "som_profiel_jaar": som_jaar,
            }
        }

    fractie = som_periode / som_jaar
    diagnose = {
        "profielNaam": inp.profiel_naam,
        "afnameKwh": inp.afname_kwh,
        "periodeVan": inp.periode_van,
        "periodeTot": inp.periode_tot,
        "dagenInPeriode": dagen,
        "som_profiel_periode": round(som_periode, 4),
        "som_profiel_jaar": round(som_jaar, 4),
        "profielFractieInPeriode": round(fractie, 6),
        "weekdag_mapping": "weekdag_bewust",
    }

    if dagen < MIN_DAGEN_BETROUWBAAR:
        return {
            "ok": False, "status": "ONBETROUWBAAR",
            "reden": (
                f"Factuurperiode ({dagen} dagen) is korter dan minimum "
                f"{MIN_DAGEN_BETROUWBAAR} dagen voor betrouwbare projectie."
            ),
            "_diagnose": diagnose,
        }

    if fractie <= 0:
        return {
            "ok": False, "status": "FOUT",
            "reden": "Profielfractie in periode = 0 — kan niet projecteren.",
            "_diagnose": diagnose,
        }

    # Projectie: als fractie X% van jaarprofiel valt in factuurperiode, dan
    # wordt het volledige geprojecteerde jaarverbruik = afnameKwh / fractie.
    geproj_kwh = inp.afname_kwh / fractie
    geproj_mwh = round(geproj_kwh / 1000, 2)

    tier = _kies_tier(geproj_mwh, staffel)
    if tier is None:
        return {
            "ok": False, "status": "FOUT",
            "reden": f"Geen tier gevonden voor jaarverbruik {geproj_mwh} MWh.",
            "_diagnose": diagnose,
        }

    return {
        "ok": True,
        "geprojecteerdJaarverbruikMWh": geproj_mwh,
        "tier": tier,
        "_diagnose": diagnose,
    }


# ─── CLI / TEST RUNNER ───────────────────────────────────────────────────────

def _laad_profiel(profielen_dir: str, naam: str) -> list[float]:
    """Lees data/profielen/<naam>.json (case-insensitive). Geeft lijst van 35040 floats."""
    if not os.path.isdir(profielen_dir):
        raise FileNotFoundError(f"profielen-dir niet gevonden: {profielen_dir}")
    target = naam.lower() + ".json"
    for fn in os.listdir(profielen_dir):
        if fn.lower() == target:
            fp = os.path.join(profielen_dir, fn)
            with open(fp, "r", encoding="utf-8") as f:
                d = json.load(f)
            arr = d if isinstance(d, list) else d.get("profiel_kwartier", [])
            return [float(x) for x in arr]
    raise FileNotFoundError(f"profiel '{naam}' niet gevonden in {profielen_dir}")


if __name__ == "__main__":
    # Smoke-test met synthetisch profiel (vlakke 0.001 / kwartier).
    flat = [0.001] * KWARTIEREN_2025
    res = project_jaarverbruik(ProjectieInput(
        profiel_naam="TEST-FLAT",
        profiel_kwartier=flat,
        afname_kwh=22932,
        periode_van="2026-01-01",
        periode_tot="2026-01-31",
    ))
    print(json.dumps(res, indent=2, ensure_ascii=False))
