#!/usr/bin/env python3
"""
Fluctus Battery Dispatch Simulator v1.1
========================================
Lees JSON van stdin, schrijf JSON naar stdout.

Wijzigingen v1.1:
  - Leveringscontract werkt nu met staffel (Enwyse-stijl)
  - Markup/markdown worden auto-bepaald op basis van jaarverbruik_mwh
  - Vergroening (€/MWh) als aparte regel in groep A
  - Imbalance markup/markdown is fallback wanneer modus=forfaitair
  - Geen floor op injectievergoeding (kan negatief zijn)
  - Backwards-compatible: oude markup_eur_mwh/markdown_eur_mwh blijven werken

Input-structuur (top-level keys):
  - profiel_kwartier: list[35040]   (genormaliseerd, som=1.0, basisprofiel)
  - aanvullingen: { laadinfra: {...}|null, elektrificatie: {...}|null }
  - jaarverbruik_mwh: float          (basisprofiel volume)
  - pv: { kwp, specifiek_rendement_kwh_per_kwp, vorm_kwartier[35040], capex_eur }
  - batterij: { kw, kwh, dod_pct, rte_pct, capex_eur, max_cycli }
  - aansluiting: { max_afname_kw_zacht, max_afname_kw_hard,
                   max_injectie_kw_zacht, max_injectie_kw_hard,
                   tarief_overschrijding_afname_eur_per_kw_jaar,
                   tarief_overschrijding_injectie_eur_per_kw_jaar }
  - contract: { modus, markup_eur_mwh, markdown_eur_mwh,
                imb_forfait_afname, imb_forfait_injectie,
                vaste_kost_eur_maand, gsc_eur_mwh, wkk_eur_mwh,
                vergroening_eur_per_mwh, staffel,
                injectie_toegelaten }
                # NIEUW v1.1:
                #   staffel = list[{min_mwh, max_mwh, consumption_dam_markup,
                #                   consumption_imbalance_markup,
                #                   injection_dam_markdown,
                #                   injection_imbalance_markdown}]
                #   vergroening_eur_per_mwh = float
                # Als staffel aanwezig is wordt markup/markdown afgeleid uit schijf.
                # Als staffel ontbreekt valt het terug op markup_eur_mwh/markdown_eur_mwh.
  - netbeheer: { grd, spanning, tarieven: {...complete tariefset uit Overzicht 2026...} }
  - forecast: { sigma_da, sigma_imb, sigma_volume_verbruik_pct, sigma_volume_pv_pct }
  - markt: { spot_kwartier[N], imb_kwartier[N], timestamps[N] }
  - simulatieperiode: { van: "YYYY-MM-DD", tot: "YYYY-MM-DD" }
  - random_seed: int (default 42, voor deterministische ruis)

Output-structuur: zie scenario-JSON spec in instructie.
"""

import sys
import json
import math
import logging
from datetime import datetime, timedelta
from typing import Any
import random

import pulp

# Logging gaat naar stderr (stdout is gereserveerd voor JSON-output)
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)


# =============================================================================
# UTILITIES
# =============================================================================

def parse_iso_date(s: str) -> datetime:
    """Parse YYYY-MM-DD naar datetime op middernacht."""
    return datetime.strptime(s, '%Y-%m-%d')


def quarter_index_in_year_2025(dt: datetime) -> int:
    """
    Voor weekdag-aligned profiel-lookup. Profielen starten op wo 1/1/2025.
    Gegeven een sim-datum, vind het kwartier-index in het 2025-profiel met:
      1. zelfde maand+dag
      2. weekdag matchend (schuif ±1, ±2, ±3 dagen tot match)
    """
    target_weekday = dt.weekday()  # 0=ma, 6=zo
    # Start: zelfde maand+dag in 2025
    try:
        candidate = datetime(2025, dt.month, dt.day, dt.hour, dt.minute)
    except ValueError:
        # 29 feb in een schrikkeljaar dat niet 2025 is → val terug op 28 feb
        candidate = datetime(2025, 2, 28, dt.hour, dt.minute)

    if candidate.weekday() == target_weekday:
        offset_days = 0
    else:
        # Probeer ±1, ±2, ±3 dagen
        offset_days = None
        for delta in [1, -1, 2, -2, 3, -3]:
            shifted = candidate + timedelta(days=delta)
            if shifted.weekday() == target_weekday:
                offset_days = delta
                break
        if offset_days is None:
            offset_days = 0  # fallback: geen match binnen ±3 dagen, gebruik origineel
        candidate = candidate + timedelta(days=offset_days)

    # Bereken kwartier-index in 2025: (dag_van_jaar - 1) * 96 + uur*4 + min/15
    jan1_2025 = datetime(2025, 1, 1)
    minutes_diff = (candidate - jan1_2025).total_seconds() / 60.0
    quarter_idx = int(minutes_diff / 15.0)
    # Begrens
    if quarter_idx < 0:
        quarter_idx = 0
    if quarter_idx >= 35040:
        quarter_idx = 35040 - 1
    return quarter_idx


def add_gaussian_noise(values: list, sigma: float, rng: random.Random) -> list:
    """Voeg Gaussiaanse ruis toe aan elke waarde (absoluut, niet pct)."""
    if sigma <= 0:
        return list(values)
    return [v + rng.gauss(0, sigma) for v in values]


def add_relative_noise(values: list, sigma_pct: float, rng: random.Random) -> list:
    """Voeg relatieve Gaussiaanse ruis toe (sigma als percentage van de waarde)."""
    if sigma_pct <= 0:
        return list(values)
    sigma_frac = sigma_pct / 100.0
    return [v * (1 + rng.gauss(0, sigma_frac)) for v in values]


# =============================================================================
# PROFIEL-OPBOUW
# =============================================================================

def build_consumption_profile(
    basis_profiel: list,          # 35040 waarden, som=1.0
    jaarverbruik_mwh: float,
    aanvullingen: dict,            # { laadinfra, elektrificatie }
    sim_timestamps: list,          # list[datetime] van N kwartieren in sim-periode
) -> list:
    """
    Bouw verbruiksprofiel in kW voor sim-periode.
    Returns: list[N] met kW-waarden per kwartier.
    """
    N = len(sim_timestamps)
    consumption_kw = [0.0] * N

    # Basisprofiel: kWh per kwartier = basis_profiel[idx_2025] * jaarverbruik_kwh
    jaarverbruik_kwh = jaarverbruik_mwh * 1000.0
    for i, ts in enumerate(sim_timestamps):
        idx2025 = quarter_index_in_year_2025(ts)
        kwh_kwartier = basis_profiel[idx2025] * jaarverbruik_kwh
        # kW = kWh / 0.25h
        consumption_kw[i] = kwh_kwartier * 4.0

    # Aanvulling laadinfra
    if aanvullingen and aanvullingen.get('laadinfra'):
        laadinfra = aanvullingen['laadinfra']
        prof = laadinfra.get('profiel_kwartier', [])
        vol_mwh = laadinfra.get('jaarvolume_mwh', 0)
        if prof and vol_mwh > 0 and len(prof) == 35040:
            vol_kwh = vol_mwh * 1000.0
            for i, ts in enumerate(sim_timestamps):
                idx2025 = quarter_index_in_year_2025(ts)
                kwh_kwartier = prof[idx2025] * vol_kwh
                consumption_kw[i] += kwh_kwartier * 4.0

    # Aanvulling elektrificatie
    if aanvullingen and aanvullingen.get('elektrificatie'):
        elektr = aanvullingen['elektrificatie']
        prof = elektr.get('profiel_kwartier', [])
        vol_mwh = elektr.get('jaarvolume_mwh', 0)
        if prof and vol_mwh > 0 and len(prof) == 35040:
            vol_kwh = vol_mwh * 1000.0
            for i, ts in enumerate(sim_timestamps):
                idx2025 = quarter_index_in_year_2025(ts)
                kwh_kwartier = prof[idx2025] * vol_kwh
                consumption_kw[i] += kwh_kwartier * 4.0

    return consumption_kw


def build_pv_profile(
    pv_vorm: list,                 # 35040 of N waarden, genormaliseerd op 1 over de periode
    kwp: float,
    specifiek_rendement: float,    # kWh/kWp/jaar
    sim_timestamps: list,
) -> list:
    """
    Returns: list[N] PV-productie in kW per kwartier.
    """
    N = len(sim_timestamps)
    if not pv_vorm or kwp <= 0:
        return [0.0] * N

    jaarproductie_kwh = kwp * specifiek_rendement

    # PV-vorm kan al N waarden zijn (uit Elia cache, sim-periode-specifiek) of 35040 (jaar).
    # We assumen sim-periode-specifiek (door Node geleverd), genormaliseerd op 1.
    if len(pv_vorm) == N:
        # Direct gebruiken
        return [pv_vorm[i] * jaarproductie_kwh * 4.0 for i in range(N)]
    elif len(pv_vorm) == 35040:
        # Project naar sim-periode via weekdag-alignment, daarna her-normaliseren
        prof_proj = [pv_vorm[quarter_index_in_year_2025(ts)] for ts in sim_timestamps]
        som = sum(prof_proj)
        if som > 0:
            prof_proj = [v / som for v in prof_proj]
        return [v * jaarproductie_kwh * 4.0 for v in prof_proj]
    else:
        # Onbekend formaat → return nullen
        log.warning(f"PV-vorm heeft {len(pv_vorm)} waarden, verwacht {N} of 35040. PV gezet op 0.")
        return [0.0] * N


# =============================================================================
# LEVERINGSCONTRACT — STAFFEL HELPER (v1.1)
# =============================================================================

def pick_schijf(jaarverbruik_mwh: float, staffel: list) -> dict:
    """
    Selecteer de juiste staffel-schijf op basis van jaarverbruik (in MWh).
    Schijven hebben min_mwh (inclusief) en max_mwh (exclusief).
    Returns: dict met de gekozen schijf, of None als geen match.
    """
    if not staffel:
        return None
    for schijf in staffel:
        mn = float(schijf.get('min_mwh', 0))
        mx = float(schijf.get('max_mwh', 1e12))
        if mn <= jaarverbruik_mwh < mx:
            return schijf
    # Fallback: laatste schijf (>1000)
    return staffel[-1]


def resolve_contract_pricing(contract: dict, jaarverbruik_mwh: float) -> dict:
    """
    Hydrateer markup/markdown uit staffel als die aanwezig is, anders gebruik
    de oude markup_eur_mwh/markdown_eur_mwh keys (backwards compatible).

    Returns een dict met expliciete velden voor de LP-loop:
      markup_dam, markdown_dam, markup_imb, markdown_imb,
      vergroening, vaste_kost_maand, modus
    """
    out = {}
    staffel = contract.get('staffel') or []
    schijf = pick_schijf(jaarverbruik_mwh, staffel) if staffel else None

    if schijf is not None:
        out['markup_dam'] = float(schijf.get('consumption_dam_markup', 0.0))
        out['markdown_dam'] = float(schijf.get('injection_dam_markdown', 0.0))
        out['markup_imb'] = float(schijf.get('consumption_imbalance_markup', 0.0))
        out['markdown_imb'] = float(schijf.get('injection_imbalance_markdown', 0.0))
        out['schijf_code'] = schijf.get('code', '')
        out['schijf_label'] = schijf.get('label', '')
    else:
        # Fallback op oude keys
        out['markup_dam'] = float(contract.get('markup_eur_mwh', 0.0))
        out['markdown_dam'] = float(contract.get('markdown_eur_mwh', 0.0))
        out['markup_imb'] = float(contract.get('imb_forfait_afname', 0.0))
        out['markdown_imb'] = float(contract.get('imb_forfait_injectie', 0.0))
        out['schijf_code'] = ''
        out['schijf_label'] = '(legacy)'

    out['vergroening'] = float(contract.get('vergroening_eur_per_mwh', 0.0))
    out['vaste_kost_maand'] = float(contract.get('vaste_kost_eur_maand', 0.0))
    out['modus'] = contract.get('modus', 'passthrough')  # 'passthrough' | 'forfaitair'
    out['gsc'] = float(contract.get('gsc_eur_mwh', 0.0))
    out['wkk'] = float(contract.get('wkk_eur_mwh', 0.0))
    return out


# =============================================================================
# LP-DISPATCH PER DAG
# =============================================================================

def lp_dispatch_day(
    consumption_kw: list,         # 96 kwartieren
    pv_kw: list,                  # 96 kwartieren
    spot_eur_mwh: list,           # 96 kwartieren (forecast)
    soc_start_kwh: float,         # SoC bij start van de dag
    batterij: dict,               # kw, kwh, dod_pct, rte_pct, capex, max_cycli
    aansluiting: dict,
    contract: dict,
    cyclus_kost_eur_per_kwh: float,
) -> dict:
    """
    Run LP voor 96 kwartieren. Maximaliseer NPV - cyclus-kost - penalties.
    Returns: { p_charge[96], p_discharge[96], grid_in[96], grid_out[96], soc[97] }
    """
    H = 96
    dt_h = 0.25

    kw_batt = batterij['kw']
    kwh_batt = batterij['kwh']
    dod = batterij['dod_pct'] / 100.0 if batterij['dod_pct'] > 1.5 else batterij['dod_pct']
    rte = batterij['rte_pct'] / 100.0 if batterij['rte_pct'] > 1.5 else batterij['rte_pct']
    eta = math.sqrt(rte)  # symmetrische η voor laden/ontladen

    soc_min = 0.0  # absolute kWh
    soc_max = kwh_batt * dod

    # Conversie van €/kW/jaar naar €/kWh-equivalent voor LP-penalty.
    # Bedacht zo: een overschrijding van 1 kW gedurende 1 kwartier = 0.25 kWh.
    # Tarief €X/kW/jaar betekent: als je 1 kW het hele jaar overschrijdt, betaal je €X.
    # Per kwartier: €X / (8760 * 4) ≈ €X / 35040 per kWh-equivalent.
    # Maar dat is te zwak — werkelijk Vlaams tarief rekent op gemiddelde maandpiek.
    # Pragmatische LP-keuze: zachte penalty per overschrijdingsmoment in €/kW (niet /jaar).
    # We gebruiken het jaartarief gedeeld door 12 (per maand-piek-equivalent) en passen
    # toe per kwartier-overschrijdingsmoment. Dit is een benadering.
    tar_afname_kw = aansluiting.get('tarief_overschrijding_afname_eur_per_kw_jaar', 62.47)
    tar_injectie_kw = aansluiting.get('tarief_overschrijding_injectie_eur_per_kw_jaar', 1.0)

    pen_afname_zacht = tar_afname_kw / 12.0   # €/kW per overschrijdingskwartier (benadering)
    pen_afname_hard = 100000.0 / 12.0
    pen_injectie_zacht = tar_injectie_kw / 12.0
    pen_injectie_hard = 100000.0 / 12.0

    max_afname_zacht = aansluiting.get('max_afname_kw_zacht', 1e9)
    max_afname_hard = aansluiting.get('max_afname_kw_hard', 1e9)
    max_injectie_zacht = aansluiting.get('max_injectie_kw_zacht', 1e9)
    max_injectie_hard = aansluiting.get('max_injectie_kw_hard', 1e9)

    injectie_toegelaten = contract.get('injectie_toegelaten', True)
    if not injectie_toegelaten:
        max_injectie_zacht = 0.0
        max_injectie_hard = 0.0

    # Markup/markdown via staffel (v1.1) — resolve_contract_pricing kiest
    # automatisch de juiste schijf, of valt terug op legacy markup_eur_mwh.
    jaarverbruik_voor_pricing = contract.get('jaarverbruik_mwh', 0.0)
    if not jaarverbruik_voor_pricing:
        # Schat uit consumption_kw (deze functie krijgt 96-kwartier dag, dus extrapoleer)
        jaarverbruik_voor_pricing = sum(consumption_kw) * 0.25 * 365 / 1000.0
    pricing = resolve_contract_pricing(contract, jaarverbruik_voor_pricing)

    if pricing['modus'] == 'forfaitair':
        # Imbalance fallback: één forfait per MWh, geen passthrough
        markup_per_mwh = pricing['markup_imb']
        markdown_per_mwh = pricing['markdown_imb']
    else:
        # Passthrough (DAM): nominatie OK
        markup_per_mwh = pricing['markup_dam']
        markdown_per_mwh = pricing['markdown_dam']

    vergroening = pricing['vergroening']
    gsc = pricing['gsc']
    wkk = pricing['wkk']

    # LP setup
    prob = pulp.LpProblem('battery_dispatch', pulp.LpMinimize)

    p_ch = [pulp.LpVariable(f'pch_{t}', 0, kw_batt) for t in range(H)]
    p_dis = [pulp.LpVariable(f'pdis_{t}', 0, kw_batt) for t in range(H)]
    grid_in = [pulp.LpVariable(f'gin_{t}', 0, max_afname_hard) for t in range(H)]
    grid_out = [pulp.LpVariable(f'gout_{t}', 0, max_injectie_hard) for t in range(H)]
    soc = [pulp.LpVariable(f'soc_{t}', soc_min, soc_max) for t in range(H + 1)]
    over_afn_zacht = [pulp.LpVariable(f'oaz_{t}', 0) for t in range(H)]
    over_afn_hard = [pulp.LpVariable(f'oah_{t}', 0) for t in range(H)]
    over_inj_zacht = [pulp.LpVariable(f'oiz_{t}', 0) for t in range(H)]
    over_inj_hard = [pulp.LpVariable(f'oih_{t}', 0) for t in range(H)]

    # Initiële SoC
    prob += soc[0] == soc_start_kwh

    # Power balance per kwartier:
    # consumption = pv + grid_in - grid_out + p_dis - p_ch
    for t in range(H):
        prob += grid_in[t] - grid_out[t] + p_dis[t] - p_ch[t] + pv_kw[t] == consumption_kw[t]
        # SoC update
        prob += soc[t + 1] == soc[t] + eta * p_ch[t] * dt_h - (1.0 / eta) * p_dis[t] * dt_h
        # Overschrijding-tracking (lineaire activatie):
        #   over_afn_zacht = max(0, grid_in - max_zacht)
        #   over_afn_hard  = max(0, grid_in - max_hard)
        # Beide zijn ondergrens-constraints; LP minimaliseert ze in objective dus ze worden
        # zo klein mogelijk. We willen NIET de eerste constraint OOK voor 'hard' opleggen.
        prob += over_afn_zacht[t] >= grid_in[t] - max_afname_zacht
        prob += over_afn_hard[t] >= grid_in[t] - max_afname_hard
        prob += over_inj_zacht[t] >= grid_out[t] - max_injectie_zacht
        prob += over_inj_hard[t] >= grid_out[t] - max_injectie_hard
        # Cosφ-constraint (gebundeld): laden + ontladen + grid niet allemaal tegelijk op max
        # Vereenvoudiging: omvormer-cap p_ch + p_dis ≤ kw_batt (mutually exclusive in praktijk)
        prob += p_ch[t] + p_dis[t] <= kw_batt

    # Objective: minimaliseer kost
    obj_terms = []
    for t in range(H):
        # Energiekost afname (€/kwartier): kW * 0.25 * (€/MWh) / 1000
        # v1.1: vergroening (€/MWh) wordt opgeteld bij afnameprijs.
        # Geen floor op injectieprijs — kan negatief zijn bij negatieve spot.
        prijs_afn_t = (spot_eur_mwh[t] + markup_per_mwh + gsc + wkk + vergroening) / 1000.0
        prijs_inj_t = (spot_eur_mwh[t] - markdown_per_mwh) / 1000.0
        obj_terms.append(prijs_afn_t * grid_in[t] * dt_h)
        obj_terms.append(-prijs_inj_t * grid_out[t] * dt_h)
        # Cyclus-kost: ontladen kost
        obj_terms.append(cyclus_kost_eur_per_kwh * p_dis[t] * dt_h)
        # Penalty's (let op: zacht is alleen over zacht-drempel, hard is over hard-drempel,
        # dus dubbeltelling tussen zachte band en harde band: alleen het zachte deel telt zacht,
        # alleen het deel boven hard telt extra hard)
        # We minimaliseren over_afn_zacht (= alles boven zacht), maar trekken het hard-deel
        # niet af omdat over_afn_hard apart geteld wordt. Dat geeft effectief:
        #   tot_pen = pen_zacht * over_zacht + pen_hard * over_hard
        # waarbij over_zacht ≥ over_hard (alles boven hard valt ook boven zacht).
        # Dat is OK: het hard-deel wordt extra belast, het zacht-deel betaalt zacht-tarief.
        obj_terms.append(pen_afname_zacht * over_afn_zacht[t])
        obj_terms.append(pen_afname_hard * over_afn_hard[t])
        obj_terms.append(pen_injectie_zacht * over_inj_zacht[t])
        obj_terms.append(pen_injectie_hard * over_inj_hard[t])

    prob += pulp.lpSum(obj_terms)

    solver = pulp.PULP_CBC_CMD(msg=0, timeLimit=10)
    status = prob.solve(solver)

    if pulp.LpStatus[status] not in ('Optimal', 'Not Solved'):
        log.warning(f"LP-status: {pulp.LpStatus[status]}")

    return {
        'p_charge': [pulp.value(v) or 0.0 for v in p_ch],
        'p_discharge': [pulp.value(v) or 0.0 for v in p_dis],
        'grid_in': [pulp.value(v) or 0.0 for v in grid_in],
        'grid_out': [pulp.value(v) or 0.0 for v in grid_out],
        'soc': [pulp.value(v) or 0.0 for v in soc],
        'over_afn_zacht': [pulp.value(v) or 0.0 for v in over_afn_zacht],
        'over_afn_hard': [pulp.value(v) or 0.0 for v in over_afn_hard],
        'over_inj_zacht': [pulp.value(v) or 0.0 for v in over_inj_zacht],
        'over_inj_hard': [pulp.value(v) or 0.0 for v in over_inj_hard],
    }


# =============================================================================
# JAARFACTUUR (volgens Facturatielogica.xlsx)
# =============================================================================

def bereken_jaarfactuur(
    grid_in_kw: list,              # N kwartieren
    grid_out_kw: list,             # N kwartieren
    spot_eur_mwh: list,            # N kwartieren (werkelijk, niet forecast)
    imb_eur_mwh: list,             # N kwartieren (werkelijk, alleen relevant voor passthrough)
    timestamps: list,              # list[datetime], N kwartieren
    contract: dict,
    netbeheer: dict,
    n_maanden: int = 12,
) -> dict:
    """
    Bereken jaarfactuur volgens groepen A-E.
    Returns: dict met groepen en totaal.
    """
    N = len(grid_in_kw)
    dt_h = 0.25

    tarieven = netbeheer['tarieven']
    spanning = netbeheer['spanning']  # 'MS' of 'LS'

    # Afname totaal in MWh
    afname_kwh_per_kwartier = [grid_in_kw[i] * dt_h for i in range(N)]
    injectie_kwh_per_kwartier = [grid_out_kw[i] * dt_h for i in range(N)]
    totaal_afname_mwh = sum(afname_kwh_per_kwartier) / 1000.0
    totaal_injectie_mwh = sum(injectie_kwh_per_kwartier) / 1000.0

    # Maandindexering
    maand_kw_afname = [[] for _ in range(12)]
    maand_kw_injectie = [[] for _ in range(12)]
    maand_mwh_afname = [0.0] * 12
    maand_mwh_injectie = [0.0] * 12
    for i, ts in enumerate(timestamps):
        m = ts.month - 1
        maand_kw_afname[m].append(grid_in_kw[i])
        maand_kw_injectie[m].append(grid_out_kw[i])
        maand_mwh_afname[m] += afname_kwh_per_kwartier[i] / 1000.0
        maand_mwh_injectie[m] += injectie_kwh_per_kwartier[i] / 1000.0

    # Maandpieken (kW, ceil)
    maandpieken_afname = [math.ceil(max(m)) if m else 0 for m in maand_kw_afname]
    maandpieken_injectie = [math.ceil(max(m)) if m else 0 for m in maand_kw_injectie]

    # Jaarpiek (Elia: jan-mrt + nov-dec winter)
    winter_kw = []
    for i, ts in enumerate(timestamps):
        if ts.month in [1, 2, 3, 11, 12]:
            winter_kw.append(grid_in_kw[i])
    jaarpiek_afname = math.ceil(max(winter_kw)) if winter_kw else 0
    toegangsvermogen = max(maandpieken_afname)  # = max(maandpieken)

    # ---- GROEP A: ENERGIEKOST (commodity) ----
    # v1.1: markup/markdown uit staffel (Enwyse-stijl) als die aanwezig is.
    pricing = resolve_contract_pricing(contract, totaal_afname_mwh)
    A = {}
    # A1. Afname energie spot (kost = sum(kWh × spot/1000))
    A['afname_energie_spot'] = sum(
        afname_kwh_per_kwartier[i] * spot_eur_mwh[i] / 1000.0 for i in range(N)
    )
    # A2. Markup afname (DAM-tarief uit schijf)
    A['markup_afname'] = totaal_afname_mwh * pricing['markup_dam']
    # A3. Vergroening van verbruik (€/MWh × MWh afname)
    A['vergroening'] = totaal_afname_mwh * pricing['vergroening']
    # A4. GSC (groene stroom certificaten — meestal 0 bij Enwyse-contract)
    A['gsc'] = totaal_afname_mwh * pricing['gsc']
    # A5. WKK (warmte-krachtkoppeling — meestal 0 bij Enwyse-contract)
    A['wkk'] = totaal_afname_mwh * pricing['wkk']
    # A6. Imbalance afname
    if pricing['modus'] == 'forfaitair':
        # Forfaitair: imbalance markup als €/MWh op het volume
        A['imbalance_afname'] = totaal_afname_mwh * pricing['markup_imb']
    else:
        # Passthrough: per kwartier (imb - spot) × kWh
        A['imbalance_afname'] = sum(
            afname_kwh_per_kwartier[i] * (imb_eur_mwh[i] - spot_eur_mwh[i]) / 1000.0
            for i in range(N)
        )
    # A7. Vaste kost leverancier (€/maand × 12)
    A['vaste_kost_leverancier'] = pricing['vaste_kost_maand'] * 12
    # A8. Injectie energie spot (negatief = inkomst)
    A['injectie_energie_spot'] = -sum(
        injectie_kwh_per_kwartier[i] * spot_eur_mwh[i] / 1000.0 for i in range(N)
    )
    # A9. Markdown injectie (positief, want minder inkomst)
    A['markdown_injectie'] = totaal_injectie_mwh * pricing['markdown_dam']
    # A10. Imbalance injectie
    if pricing['modus'] == 'forfaitair':
        A['imbalance_injectie'] = totaal_injectie_mwh * pricing['markdown_imb']
    else:
        A['imbalance_injectie'] = -sum(
            injectie_kwh_per_kwartier[i] * (imb_eur_mwh[i] - spot_eur_mwh[i]) / 1000.0
            for i in range(N)
        )
    A['_subtotaal'] = sum(v for k, v in A.items() if not k.startswith('_'))
    A['_meta'] = {
        'leverancier': contract.get('leverancier', 'onbekend'),
        'schijf_code': pricing.get('schijf_code', ''),
        'schijf_label': pricing.get('schijf_label', ''),
        'markup_eur_mwh': pricing['markup_dam'],
        'markdown_eur_mwh': pricing['markdown_dam'],
        'modus': pricing['modus'],
    }

    # ---- GROEP B: NETGEBRUIK AFNAME ----
    B = {}
    tar_maandpiek = tarieven.get('maandpiek_eur_kw_jaar', 0.0)
    tar_jaarpiek = tarieven.get('toegangsvermogen_eur_kw_jaar', 0.0)
    tar_overschr = tarieven.get('overschrijding_toegangsvermogen_eur_kw_jaar', 0.0)
    tar_prop = tarieven.get('proportioneel_eur_mwh', 0.0)
    tar_databeheer = tarieven.get('databeheer_eur_jaar', 0.0)
    tar_reactief = tarieven.get('reactief_eur_mvarh', 0.0)

    # Maandpiek kost (gemiddelde maandpiek voor LS, som maandpieken voor MS):
    # Voor MS: tarief in €/kW/jaar betekent: gemiddelde maandpiek × tarief / 12 × 12 = gemiddelde × tarief.
    # Voor LS: idem (Vlaanderen), of "gemiddelde maandpiek" formule.
    # Vereenvoudiging v1: gemiddelde van de 12 maandpieken × tarief (€/kW/jaar).
    if maandpieken_afname:
        gem_maandpiek = sum(maandpieken_afname) / len(maandpieken_afname)
    else:
        gem_maandpiek = 0
    B['maandpiek'] = gem_maandpiek * tar_maandpiek
    # Jaarpiek (toegangsvermogen)
    B['toegangsvermogen'] = toegangsvermogen * tar_jaarpiek
    # Overschrijding toegangsvermogen (gemiddelde van max(0, maandpiek - toegangsvermogen) × tarief)
    overschr = [max(0, p - toegangsvermogen) for p in maandpieken_afname]
    if overschr:
        gem_overschr = sum(overschr) / len(overschr)
    else:
        gem_overschr = 0
    B['overschrijding_toegangsvermogen'] = gem_overschr * tar_overschr
    # Proportioneel kWh
    B['proportioneel'] = totaal_afname_mwh * tar_prop
    # Reactief: cosφ=1 in v1 → 0
    B['reactief'] = 0.0
    # Databeheer
    B['databeheer'] = tar_databeheer
    B['_subtotaal'] = sum(v for k, v in B.items() if not k.startswith('_'))

    # ---- GROEP C: NETGEBRUIK INJECTIE ----
    C = {}
    tar_inj_prop = tarieven.get('injectie_proportioneel_eur_mwh', 0.0)
    tar_inj_cap = tarieven.get('injectie_capaciteit_eur_kva_maand', 0.0)
    tar_inj_databeheer = tarieven.get('injectie_databeheer_eur_jaar', 0.0)
    tar_inj_vaste = tarieven.get('injectie_vaste_vergoeding_eur_jaar', 0.0)
    C['proportioneel'] = totaal_injectie_mwh * tar_inj_prop
    # Capaciteit injectie: gemiddelde maandpiek injectie × tarief × 12
    if maandpieken_injectie:
        gem_maandpiek_inj = sum(maandpieken_injectie) / len(maandpieken_injectie)
    else:
        gem_maandpiek_inj = 0
    C['capaciteit_injectie'] = gem_maandpiek_inj * tar_inj_cap * 12
    C['databeheer_injectie'] = tar_inj_databeheer
    C['vaste_vergoeding'] = tar_inj_vaste
    C['_subtotaal'] = sum(v for k, v in C.items() if not k.startswith('_'))

    # ---- GROEP D: TRANSPORT (Elia, indien apart) ----
    D = {}
    tar_tr_maandpiek = tarieven.get('transport_maandpiek_eur_kw_mnd', 0.0)
    tar_tr_jaarpiek = tarieven.get('transport_jaarpiek_eur_kw_jaar', 0.0)
    tar_tr_systeem = tarieven.get('transport_systeembeheer_eur_mwh', 0.0)
    tar_tr_reserves = tarieven.get('transport_reserves_eur_mwh', 0.0)
    tar_tr_markt = tarieven.get('transport_marktintegratie_eur_mwh', 0.0)
    tar_tr_beschikb = tarieven.get('transport_beschikbaar_eur_kva_jaar', 0.0)
    tar_tr_reactief = tarieven.get('transport_reactief_eur_mvarh', 0.0)

    # Maandpiek transport: som van maandpieken × tarief/maand
    D['maandpiek_transport'] = sum(maandpieken_afname) * tar_tr_maandpiek
    D['jaarpiek_transport'] = jaarpiek_afname * tar_tr_jaarpiek
    D['systeembeheer'] = totaal_afname_mwh * tar_tr_systeem
    D['reserves'] = totaal_afname_mwh * tar_tr_reserves
    D['marktintegratie'] = totaal_afname_mwh * tar_tr_markt
    D['beschikbaar_vermogen'] = toegangsvermogen * tar_tr_beschikb
    D['reactief_transport'] = 0.0  # cosφ=1
    D['_subtotaal'] = sum(v for k, v in D.items() if not k.startswith('_'))

    # ---- GROEP E: HEFFINGEN ----
    E = {}
    E['odv_osp'] = totaal_afname_mwh * tarieven.get('odv_eur_mwh', 0.0)
    E['surcharges'] = totaal_afname_mwh * tarieven.get('surcharges_eur_mwh', 0.0)
    E['soldes_regulatoires'] = totaal_afname_mwh * tarieven.get('soldes_eur_mwh', 0.0)

    # Accijnzen-staffel (cumulatief per schijf) - ZAKELIJKE TARIEVEN 2026
    # Bron: Programmawet art. 419 k) 2) zakelijke sub-categorie
    # Geverifieerd via Ecopower tariefkaart januari 2026 + 10 echte facturen
    accijnzen_staffel = tarieven.get('accijnzen_staffel', [])
    # Format: [(grens_mwh, tarief_eur_mwh), ...]
    if not accijnzen_staffel:
        # Default ZAKELIJKE tarieven 2026 (NIET residentieel!)
        # Schijven cumulatief in MWh op kalenderjaarbasis
        accijnzen_staffel = [
            (3, 14.21),       # Schijf 1: 0-3 MWh/jaar
            (20, 14.21),      # Schijf 2: 3-20 MWh/jaar
            (50, 12.09),      # Schijf 3: 20-50 MWh/jaar
            (1000, 11.39),    # Schijf 4: 50-1000 MWh/jaar
            (9999999, 10.00),  # Schijf 5: >1000 MWh/jaar (te verifiëren)
        ]
    accijns_basis = tarieven.get('accijns_basis_eur_mwh', 0.0)  # voor LS: 1.9261
    accijns_totaal = totaal_afname_mwh * accijns_basis  # gewone accijns (LS only)
    rest_mwh = totaal_afname_mwh
    vorig_grens = 0
    for grens, tarief in accijnzen_staffel:
        schijf_mwh = max(0, min(rest_mwh, grens - vorig_grens))
        if schijf_mwh <= 0:
            break
        accijns_totaal += schijf_mwh * tarief
        rest_mwh -= schijf_mwh
        vorig_grens = grens
        if rest_mwh <= 0:
            break
    E['accijnzen'] = accijns_totaal

    # Energiefonds Vlaanderen (vast €/jaar, BTW-vrij)
    E['energiefonds_vlaanderen'] = tarieven.get('energiefonds_eur_jaar', 0.0)
    E['_subtotaal'] = sum(v for k, v in E.items() if not k.startswith('_'))

    # ---- TOTAAL ----
    subtotaal_excl_btw = A['_subtotaal'] + B['_subtotaal'] + C['_subtotaal'] + D['_subtotaal'] + E['_subtotaal']
    # BTW 21% op alles BEHALVE energiefonds
    btw_basis = subtotaal_excl_btw - E['energiefonds_vlaanderen']
    btw_bedrag = btw_basis * 0.21
    totaal_incl_btw = btw_basis * 1.21 + E['energiefonds_vlaanderen']

    return {
        'groepen': {
            'A_energiekost': A,
            'B_netgebruik_afname': B,
            'C_netgebruik_injectie': C,
            'D_transport': D,
            'E_heffingen': E,
        },
        'subtotaal_excl_btw': subtotaal_excl_btw,
        'btw_bedrag': btw_bedrag,
        'totaal_incl_btw': totaal_incl_btw,
        'maandpieken_afname_kw': maandpieken_afname,
        'maandpieken_injectie_kw': maandpieken_injectie,
        'toegangsvermogen_kw': toegangsvermogen,
        'jaarpiek_afname_kw': jaarpiek_afname,
        'maand_mwh_afname': maand_mwh_afname,
        'maand_mwh_injectie': maand_mwh_injectie,
    }


# =============================================================================
# HOOFDSIMULATIE
# =============================================================================

def run_simulation(inp: dict) -> dict:
    log.info("=== Fluctus Simulator v1.1 — start ===")

    rng = random.Random(inp.get('random_seed', 42))

    # ---- Sim-periode opbouwen ----
    van = parse_iso_date(inp['simulatieperiode']['van'])
    tot = parse_iso_date(inp['simulatieperiode']['tot'])
    sim_timestamps = []
    cur = van
    while cur < tot:
        sim_timestamps.append(cur)
        cur = cur + timedelta(minutes=15)
    N = len(sim_timestamps)
    log.info(f"Sim-periode: {van.date()} → {tot.date()} = {N} kwartieren ({N/96:.0f} dagen)")

    # ---- Profielen opbouwen ----
    log.info("Profielen opbouwen…")
    consumption_kw = build_consumption_profile(
        inp['profiel_kwartier'],
        inp['jaarverbruik_mwh'],
        inp.get('aanvullingen', {}),
        sim_timestamps,
    )
    log.info(f"Consumptie: max={max(consumption_kw):.1f} kW, gem={sum(consumption_kw)/N:.1f} kW, totaal={sum(consumption_kw)*0.25/1000:.1f} MWh")

    pv_kw = build_pv_profile(
        inp['pv'].get('vorm_kwartier', []),
        inp['pv'].get('kwp', 0),
        inp['pv'].get('specifiek_rendement_kwh_per_kwp', 900),
        sim_timestamps,
    )
    if max(pv_kw) > 0:
        log.info(f"PV: max={max(pv_kw):.1f} kW, totaal={sum(pv_kw)*0.25/1000:.1f} MWh")
    else:
        log.info("PV: geen productie")

    # ---- Markt-data ----
    spot_actual = inp['markt']['spot_kwartier']
    imb_actual = inp['markt'].get('imb_kwartier', spot_actual)
    if len(spot_actual) != N:
        log.warning(f"Spot heeft {len(spot_actual)} waarden, sim heeft {N} kwartieren. Truncate/pad.")
        if len(spot_actual) < N:
            spot_actual = spot_actual + [spot_actual[-1]] * (N - len(spot_actual))
            imb_actual = imb_actual + [imb_actual[-1]] * (N - len(imb_actual))
        else:
            spot_actual = spot_actual[:N]
            imb_actual = imb_actual[:N]

    # ---- Forecasts (D-1) ----
    sf = inp.get('forecast', {})
    spot_forecast = add_gaussian_noise(spot_actual, sf.get('sigma_da', 0), rng)
    consumption_forecast = add_relative_noise(consumption_kw, sf.get('sigma_volume_verbruik_pct', 0), rng)
    pv_forecast = add_relative_noise(pv_kw, sf.get('sigma_volume_pv_pct', 0), rng)

    # ---- Cyclus-kost ----
    batt = inp['batterij']
    max_cycli = batt.get('max_cycli', 8000)
    dod = batt['dod_pct'] / 100.0 if batt['dod_pct'] > 1.5 else batt['dod_pct']
    if batt['kwh'] > 0 and dod > 0 and max_cycli > 0:
        cyclus_kost = batt['capex_eur'] / (max_cycli * dod * batt['kwh'])
    else:
        cyclus_kost = 0.0
    log.info(f"Cyclus-kost: €{cyclus_kost:.4f}/kWh ontladen")

    # ---- LP per dag ----
    log.info("LP-dispatch starten (per dag, 96 kwartieren)…")
    soc_kwh = batt['kwh'] * dod * 0.5  # start halfvol
    grid_in_all = []
    grid_out_all = []
    soc_all = [soc_kwh]
    p_dis_all = []
    p_ch_all = []
    over_afn_zacht_all = []
    over_afn_hard_all = []
    over_inj_zacht_all = []
    over_inj_hard_all = []

    n_dagen = N // 96
    
    # === SNELLE PAD 1: geen batterij EN geen PV → geen LP nodig ===
    skip_lp = (batt['kwh'] <= 0 or batt['kw'] <= 0) and (max(pv_kw) if pv_kw else 0) <= 0
    # === SNELLE PAD 2: geen batterij MAAR wel PV → geen LP nodig (PV is passief) ===
    pv_only = (batt['kwh'] <= 0 or batt['kw'] <= 0) and (max(pv_kw) if pv_kw else 0) > 0
    
    if skip_lp:
        log.info("Geen batterij/PV — LP wordt overgeslagen (snelle pad)")
        zacht_afn = inp['aansluiting'].get('max_afname_kw_zacht', float('inf'))
        hard_afn = inp['aansluiting'].get('max_afname_kw_hard', float('inf'))
        for t in range(N):
            cons_kw = consumption_kw[t]
            grid_in_all.append(cons_kw)
            grid_out_all.append(0.0)
            p_dis_all.append(0.0)
            p_ch_all.append(0.0)
            soc_all.append(0.0)
            over_afn_zacht_all.append(max(0, cons_kw - zacht_afn))
            over_afn_hard_all.append(max(0, cons_kw - hard_afn))
            over_inj_zacht_all.append(0.0)
            over_inj_hard_all.append(0.0)
    elif pv_only:
        log.info("PV zonder batterij — passieve dispatch (snelle pad)")
        # PV gaat eerst naar zelfconsumptie, overschot naar net (injectie)
        zacht_afn = inp['aansluiting'].get('max_afname_kw_zacht', float('inf'))
        hard_afn = inp['aansluiting'].get('max_afname_kw_hard', float('inf'))
        zacht_inj = inp['aansluiting'].get('max_injectie_kw_zacht', float('inf'))
        hard_inj = inp['aansluiting'].get('max_injectie_kw_hard', float('inf'))
        for t in range(N):
            cons_kw = consumption_kw[t]
            pv_t = pv_kw[t]
            netto = cons_kw - pv_t  # > 0 = afname, < 0 = injectie
            if netto >= 0:
                grid_in_all.append(netto)
                grid_out_all.append(0.0)
                over_afn_zacht_all.append(max(0, netto - zacht_afn))
                over_afn_hard_all.append(max(0, netto - hard_afn))
                over_inj_zacht_all.append(0.0)
                over_inj_hard_all.append(0.0)
            else:
                injectie = -netto
                grid_in_all.append(0.0)
                grid_out_all.append(injectie)
                over_afn_zacht_all.append(0.0)
                over_afn_hard_all.append(0.0)
                over_inj_zacht_all.append(max(0, injectie - zacht_inj))
                over_inj_hard_all.append(max(0, injectie - hard_inj))
            p_dis_all.append(0.0)
            p_ch_all.append(0.0)
            soc_all.append(0.0)
    else:
        for d in range(n_dagen):
            i0 = d * 96
            i1 = i0 + 96
            result = lp_dispatch_day(
                consumption_forecast[i0:i1],
                pv_forecast[i0:i1],
                spot_forecast[i0:i1],
                soc_kwh,
                batt,
                inp['aansluiting'],
                inp['contract'],
                cyclus_kost,
            )
            grid_in_all.extend(result['grid_in'])
            grid_out_all.extend(result['grid_out'])
            p_dis_all.extend(result['p_discharge'])
            p_ch_all.extend(result['p_charge'])
            soc_all.extend(result['soc'][1:])
            soc_kwh = result['soc'][-1]
            over_afn_zacht_all.extend(result['over_afn_zacht'])
            over_afn_hard_all.extend(result['over_afn_hard'])
            over_inj_zacht_all.extend(result['over_inj_zacht'])
            over_inj_hard_all.extend(result['over_inj_hard'])

            if (d + 1) % 30 == 0:
                log.info(f"  LP-dispatch dag {d+1}/{n_dagen}…")

    log.info(f"LP-dispatch klaar: {n_dagen} dagen, eind-SoC = {soc_kwh:.1f} kWh")

    # ---- Werkelijke factuur (met werkelijke spot/imb, niet forecast) ----
    log.info("Jaarfactuur berekenen…")
    factuur = bereken_jaarfactuur(
        grid_in_all,
        grid_out_all,
        spot_actual,
        imb_actual,
        sim_timestamps,
        inp['contract'],
        inp['netbeheer'],
    )

    # ---- KPI's ----
    pv_naar_eigen_verbruik = sum(min(consumption_kw[i], pv_kw[i]) * 0.25 / 1000.0 for i in range(N))
    totaal_verbruik_mwh = sum(consumption_kw) * 0.25 / 1000.0
    totaal_pv_mwh = sum(pv_kw) * 0.25 / 1000.0
    pct_zelfconsumptie = (pv_naar_eigen_verbruik / totaal_verbruik_mwh * 100) if totaal_verbruik_mwh > 0 else 0
    pct_zelfvoorziening = (pv_naar_eigen_verbruik / totaal_pv_mwh * 100) if totaal_pv_mwh > 0 else 0

    aantal_overschr_zacht = sum(1 for v in over_afn_zacht_all if v > 0.01)
    aantal_overschr_hard = sum(1 for v in over_afn_hard_all if v > 0.01)
    max_overschr_zacht_kw = max(over_afn_zacht_all) if over_afn_zacht_all else 0
    max_overschr_hard_kw = max(over_afn_hard_all) if over_afn_hard_all else 0
    vereist_injectie_kw = max(grid_out_all) if grid_out_all else 0

    # Cycli verbruikt
    energie_ontladen_mwh = sum(p_dis_all) * 0.25 / 1000.0
    cycli_verbruikt = energie_ontladen_mwh * 1000.0 / (batt['kwh'] * dod) if batt['kwh'] * dod > 0 else 0
    levensduur_jaren = max_cycli / cycli_verbruikt if cycli_verbruikt > 0 else 999

    kpi = {
        'totaal_incl_btw': factuur['totaal_incl_btw'],
        'subtotaal_excl_btw': factuur['subtotaal_excl_btw'],
        'pct_zelfconsumptie': pct_zelfconsumptie,
        'pct_zelfvoorziening': pct_zelfvoorziening,
        'aantal_piek_overschrijdingen_zacht': aantal_overschr_zacht,
        'aantal_piek_overschrijdingen_hard': aantal_overschr_hard,
        'max_overschr_zacht_kw': max_overschr_zacht_kw,
        'max_overschr_hard_kw': max_overschr_hard_kw,
        'vereist_injectievermogen_kw': vereist_injectie_kw,
        'cycli_verbruikt': cycli_verbruikt,
        'levensduur_jaren': levensduur_jaren,
        'totaal_afname_mwh': totaal_verbruik_mwh,
        'totaal_pv_mwh': totaal_pv_mwh,
        'totaal_grid_in_mwh': sum(grid_in_all) * 0.25 / 1000.0,
        'totaal_grid_out_mwh': sum(grid_out_all) * 0.25 / 1000.0,
    }

    log.info(f"Totaal: €{kpi['totaal_incl_btw']:.0f} | zelfconsumptie {pct_zelfconsumptie:.1f}% | overschr zacht/hard: {aantal_overschr_zacht}/{aantal_overschr_hard}")

    # ---- Maandstaten ----
    maandstaten = []
    for m in range(12):
        if m < len(factuur['maand_mwh_afname']):
            maandstaten.append({
                'maand': m + 1,
                'afname_mwh': factuur['maand_mwh_afname'][m],
                'injectie_mwh': factuur['maand_mwh_injectie'][m],
            })

    # ---- Output JSON ----
    output = {
        'jaarfactuur': {
            'groepen': factuur['groepen'],
            'subtotaal_excl_btw': factuur['subtotaal_excl_btw'],
            'btw_bedrag': factuur['btw_bedrag'],
            'totaal_incl_btw': factuur['totaal_incl_btw'],
            'maandpieken_afname_kw': factuur['maandpieken_afname_kw'],
            'maandpieken_injectie_kw': factuur['maandpieken_injectie_kw'],
            'toegangsvermogen_kw': factuur['toegangsvermogen_kw'],
            'jaarpiek_afname_kw': factuur['jaarpiek_afname_kw'],
        },
        'kpi': kpi,
        'maandstaten': maandstaten,
        'soc_reeks': soc_all[:N],  # cap to N
        'piekoverschrijdingen': {
            'aantal_zacht': aantal_overschr_zacht,
            'aantal_hard': aantal_overschr_hard,
            'kwartieren_zacht': [
                sim_timestamps[i].isoformat() for i in range(N) if over_afn_zacht_all[i] > 0.01
            ][:100],
            'kwartieren_hard': [
                sim_timestamps[i].isoformat() for i in range(N) if over_afn_hard_all[i] > 0.01
            ][:100],
        },
        'data_periode': {
            'van': sim_timestamps[0].isoformat(),
            'tot': sim_timestamps[-1].isoformat(),
            'aantal_kwartieren': N,
        },
    }

    log.info("=== Simulator klaar ===")
    return output


# =============================================================================
# MAIN
# =============================================================================

def main():
    try:
        inp = json.load(sys.stdin)
    except Exception as e:
        log.error(f"Kon input-JSON niet lezen: {e}")
        sys.exit(1)

    try:
        out = run_simulation(inp)
    except Exception as e:
        log.error(f"Simulatie gefaald: {e}", exc_info=True)
        sys.exit(2)

    json.dump(out, sys.stdout, indent=2, default=str)


if __name__ == '__main__':
    main()
