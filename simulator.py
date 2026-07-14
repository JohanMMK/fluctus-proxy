#!/usr/bin/env python3
# ============================================================================
# FLUCTUS BATTERY DISPATCH SIMULATOR
# Versie:        v1.8.11 (per-kwartier-profielen in output voor financieel rapport)
# Wijziging v1.8.11 vs v1.8.10: output.profielen met per-kwartier arrays
#   (vermogen_aansluiting_kw, spot_prijs_eur_mwh, kost_eur_mwh) t.b.v. de heatmaps.
# Versie:        v1.8.10 (2-uurs batterij-dimensionering + auto-verhoging aansluiting)
# Wijziging v1.8.10 vs v1.8.9: (1) batterij gedimensioneerd als STANDAARD 2-uurs
#   component: kW = max(dagenergie/2, minimaal-vermogen), kWh = 2×kW. (2) Als de
#   MANUEEL gekozen batterij het laadplein niet aankan (variant 2/3), wordt het
#   toegangsvermogen automatisch verhoogd tot het haalbaar is i.p.v. dagen te
#   verliezen; de verhoging staat in output.laadplein.toegangsvermogen_verhoogd_kw.
# Versie:        v1.8.9 (overnacht/wrap-around-laadvensters)
# Wijziging v1.8.9 vs v1.8.8: laadvensters die middernacht overschrijden (v_eind
#   <= v_start, bv. 18→8u) worden nu correct ondersteund i.p.v. afgekapt. De
#   laadsessie loopt van v_start op dag D tot v_eind op dag D+1 (avonddeel +
#   ochtenddeel). Belangrijk voor depots die 's nachts laden — de nodige batterij
#   of aansluitingsverhoging valt dan fors kleiner uit.
# Versie:        v1.8.8 (batterij-dimensionering op slim laden, variant 2)
# Wijziging v1.8.8 vs v1.8.7: _laadplein_capaciteit dimensioneert de geadviseerde
#   batterij (kW + kWh) nu op SLIM laden (shift_spot = variant 2) i.p.v. op dom
#   laden (onmiddellijk). Zo matcht de batterij hoe hij in de standaardsturing
#   echt draait en vermijden we een dimensioneer-mismatch.
# Versie:        v1.8.7 (bug-fix: BSP-nominatie begrensd op fysieke aansluiting)
# Wijziging v1.8.7 vs v1.8.6: KRITIEKE FIX. In de feasibility-terugval kreeg de
#   DAM-nominatie (nom_afn/nom_inj) de big-M (1e9) mee én werd het spec_budget
#   verwijderd → de BSP-papierhandel werd onbegrensd en produceerde absurde
#   facturen (miljarden €) voor sturing 3 (onbalans). Nu: nominatie ALTIJD
#   begrensd op de fysieke aansluiting (max_afname/injectie_hard), en de
#   feasibility-solve houdt een ruim spec_budget aan i.p.v. het te verwijderen.
# Versie:        v1.8.6 (maandpiek op volle gerealiseerde piek bij overschrijding)
# Wijziging v1.8.6 vs v1.8.5: de MAANDPIEK (capaciteitstarief) wordt op de
#   VOLLEDIGE gerealiseerde piek gerekend (niet gecapt op contract) → een
#   overschrijding tilt de maandpiek mee omhoog. Toegangsvermogen + beschikbaar
#   vermogen blijven op het GECONTRACTEERDE niveau (sunk); een klant die NIET
#   verhoogt betaalt de overschrijdingspenalty (geen dubbeltelling met het
#   toegangsvermogen-tarief). Verhoogt de klant wél (verhogen-opstelling), dan is
#   het contract = het verhoogde niveau → die term draagt dan de verhogingskost
#   (verhoging × toegangsvermogen + maandpiek). Onder contract blijft alles sunk.
# Versie:        v1.8.5 (variant 1 ongestuurd = mag boven toegangsvermogen)
# Wijziging v1.8.5 vs v1.8.4: variant 1 (geen sturing) laadt de EV ONMIDDELLIJK
#   en ongetemperd door het toegangsvermogen, met batterij idle en zuiver
#   passieve dispatch → de totale afname mag BOVEN het toegangsvermogen (de
#   overschrijding wordt geregistreerd en gefactureerd). Variant 2 & 3 houden de
#   totale afname (verbruik + EV + batterij) ONDER het toegangsvermogen. Zo toont
#   de vergelijking de echte meerwaarde van sturing: binnen contract blijven i.p.v.
#   overschrijden/verhogen.
# Versie:        v1.8.4 (harde piekgrens: geen maandpiek-inflatie door sturing)
# Wijziging v1.8.4 vs v1.8.3: grid_in mag in de normale modus nooit boven de
#   NATUURLIJKE LASTPIEK (max(verbruik − PV) over de periode, begrensd op
#   contract). Zo blaast de sturing de (zekere) maandpiek niet op om (onzekere)
#   spot-arbitrage of onbalans te oogsten — waarde komt enkel uit wat ONDER de
#   bestaande lastpiek past; piekshaving eronder blijft toegelaten en beloond.
#   Toegepast in lp_dispatch_day, lp_dispatch_month_bsp (normale modus) en
#   lp_dispatch_day_stacked. Feasibility-modus behoudt big-M (last moet bediend).
# Versie:        v1.8.3 (LS/MS-verfijning capaciteitskost)
# Wijziging v1.8.3 vs v1.8.2: onderscheid gecontracteerde vs gerealiseerde
#   capaciteitskost. TOEGANGSVERMOGEN + BESCHIKBAAR VERMOGEN (MS-contracttermen;
#   op LS = 0) blijven SUNK op het gecontracteerde toegangsvermogen — de MS-
#   besparing van níet-verhogen komt via de twee-opstellingen-vergelijking. De
#   MAANDPIEK (capaciteitstarief, LS én MS) staat terug op de GEREALISEERDE
#   maandpiek (gem. van 12 maandpieken, ondergrens 2,5 kW), zodat piekshaving door
#   batterij/sturing z'n echte besparing toont (LS BatteryActive/SolarActive). De
#   LP straft weer de reële maandpiek, maar enkel met de realized-gefactureerde
#   termen (maandpiek B + transport-maandpiek D); toegangsvermogen/beschikbaar
#   zitten niet meer in de piekprikkel (sunk) → kop-ruimte tot contract vrij voor
#   arbitrage. Overschrijding (piek boven contract) blijft aan het hogere tarief.
# Versie:        v1.8.2 (capaciteit = gecontracteerd toegangsvermogen, sunk)
# Wijziging v1.8.2 vs v1.8.1: netkost op toegangsvermogen wordt aangerekend op
#   het GECONTRACTEERDE toegangsvermogen (aansluiting.max_afname_kw / factuur),
#   niet op de gerealiseerde piek. Die capaciteit is 'sunk': de sturing mag de
#   reeds-betaalde kop-ruimte volluit benutten (variant 2 & 3) zonder extra
#   netkost. Enkel piek BOVEN contract = overschrijding. BSP-LP straft nog enkel
#   piek boven contract (peak_over). Injectie: geen capaciteitskost (volluit).
#   Zonder contract-getal → fallback op gerealiseerde jaarpiek (echte EAN-
#   profielen leveren toegangsvermogen = hoogste maandpiek/12m aan).
# Versie:        v1.8.1 (laadpleinen + feasibility-first BSP-terugval)
# Wijziging v1.8.1 vs v1.8: PRIORITEIT haalbaarheid (Johan). Het maand-BSP-LP
#   valt bij infeasibiliteit (bv. krappe aansluiting + laadplein waar de batterij
#   de laadpiek energetisch niet kan dragen) NIET meer terug op een verloren maand
#   (grid_in/out=0 → €0 = onzin). Nieuw niveau 3 = HAALBAAR dispatch met gesoftende
#   fysieke aansluitings-caps (big-M + zware penalty): de last wordt altijd bediend,
#   batterij/shift maximaal ingezet, eventuele cap-overschrijding komt via
#   over_afn_hard/over_inj_hard als waarschuwing naar boven. Niveau 4 = noodval.
#   Volgorde: eerst voldoen (bestaand verbruik + laden) → modus 2 → modus 3.
# Wijziging v1.8 vs v1.7.1: nieuwe input `laadpleinen` (lijst). Per laadplein
#   (1 voertuigtype: aantal × km/jaar × kWh/km) wordt een flexibele EV-laadlast
#   opgebouwd binnen een venster (start-eind) op 5/6/7 dagen/week, begrensd door de
#   laadpunt-capaciteit. Sturing volgt de 3 varianten (onmiddellijk / shift op
#   spot+PV / shift op onbalans). Bestaande laadpleinen worden uit het basisverbruik
#   geschaald en als stuurbare last toegevoegd. Output: blok 'laadplein'. Zonder
#   `laadpleinen` is het gedrag identiek aan v1.7.1.
# Versie-basis:  v1.7.1 (3-sturingen — 'geen arbitrage'-modus voor variant 1)
# Wijziging v1.7.1 vs v1.7: nieuwe input-vlag `geen_arbitrage` (default False).
#   Wanneer True: de batterij mag zelfconsumptie + piekshaving doen maar GEEN
#   spot/IMB-arbitrage. Implementatie: vlakke dispatch-prijs (spot_forecast =
#   gemiddelde spot) + forceer plain lp_dispatch_day (stacked/BSP uit). De
#   facturatie blijft op de echte spot rekenen. Gebruikt als baseline (variant 1)
#   in de 3-sturingen-KPI. Alle bestaande paden ongewijzigd wanneer vlag afwezig.
# Geproduceerd:  2026-05-14
# Doelomgeving:  Railway (lucid-amazement-production.up.railway.app)
# Repo:          JohanMMK/fluctus-proxy (auto-deploy bij merge naar main)
# Vereist:       server.js v15.13+ (asymmetrie max_injectie_kw_hard veld)
# ============================================================================
"""
Fluctus Battery Dispatch Simulator v1.7
========================================
Sessie 7 — Strategie A: maand-niveau BSP-LP met expliciete monthly_peak-kost.

Doel: LP "voelt" de werkelijke Groep B / D capaciteit-kost in plaats van enkel
de zachte profielpiek-heuristiek penalty. LP kiest economisch optimale balans
tussen BSP-arbitrage-winst en stijging van het maandpiek-aggregaat.

Wijzigingen v1.7 vs v1.6:
  ARCHITECTUUR (BSP-LP) — lp_dispatch_month_bsp (nieuw, vervangt per-dag-loop):
  - Per kalendermaand bouwt run_simulation één LP over alle kwartieren in die
    maand (28-31 × 96 = 2688-2976 kwartieren). monthly_peak is een unieke
    LP-variabele die grid_in[t] in die maand bounded.
  - Objective bevat per-maand-share van Groep B + Groep D capaciteit-kosten:
      c_per_maand = tar_maandpiek_B / 12        (Groep B maandpiek-aggregaat)
                  + tar_tr_maandpiek_D          (Groep D transport-maandpiek)
                  + tar_jaarpiek_B / 12         (Groep B toegangsvermogen,
                                                 benadering uniform-spreid)
                  + tar_tr_beschikb_D / 12      (Groep D beschikbaar vermogen)
    monthly_peak[m] × c_per_maand wordt opgenomen in de objective.
  - SoC casadeert nu per kalendermaand (van laatste kwartier maand m naar
    eerste kwartier maand m+1). Voorheen casadeerde SoC per dag.
  - Per-dag spec_dev budget blijft behouden (n_dagen aparte LP-constraints
    binnen één maand-LP). Anti-regressie t.o.v. v1.6 paper-deviation gedrag.
  - Retry-ladder werkt nu per MAAND i.p.v. per dag:
      Niveau 0: spec_budget × 1
      Niveau 1: spec_budget × 10
      Niveau 2: spec_budget volledig verwijderd
      Niveau 3: maand verloren (alle dagen in die maand: grid_in/out = 0,
                SoC blijft constant). GEEN passieve fallback (consistent met
                v1.6 design beslissing 4.3).
  - Verwachte impact SMARTUNIT_v10 Sc4: subtotaal €14.898/jaar → ≤€13.500/jaar
    (+€1.500-2.500 extra besparing per jaar t.o.v. v1.6).

  DIAGNOSTICS — lp_diagnostics:
  - Nieuwe maand-niveau tellers: totaal_maanden, optimal_maanden,
    retry1_maanden, retry2_maanden, verloren_maanden.
  - Backwards-compat: optimal_dagen / retry1_dagen / retry2_dagen / verloren_dagen
    blijven aanwezig — afgeleid uit maand-tellers (dagen = aantal dagen in
    bijbehorende maand). Simulator.txt v1.18 badge-logica blijft werken.

  BACKWARDS-COMPAT:
  - Sc1-3 paden (skip_lp / pv_only): UNCHANGED, identieke output aan v1.6.
  - Niet-BSP LP-paden (lp_dispatch_day, lp_dispatch_day_stacked): UNCHANGED.
  - lp_dispatch_day_bsp blijft aanwezig (deprecated, niet meer aangeroepen
    vanuit run_simulation). Voor externe callers behoudt v1.6-gedrag.
  - Sc4 BSP+BESS: NIEUW gedrag (monthly_peak in objective) → andere output.

Wijzigingen v1.6 vs v1.5.2-diag:
  ARCHITECTUUR (LP) — alle drie LP-functies (lp_dispatch_day,
  lp_dispatch_day_bsp, lp_dispatch_day_stacked):
  - Verwijderd: over_afn_hard / over_inj_hard variabelen + pen_*_hard penalties.
    Reden: tegenstrijdig met hard variabele-bound = silent infeasibility mode
    (RCA defect 3 — zie RCA_sessie6_LP_bound_violation.md).
  - Behouden: over_afn_zacht / over_inj_zacht (informatief, kleine penalty op
    overschrijding van het zachte plafond — bestaand gedrag).
  - grid_out cap is nu APART van grid_in cap: max_injectie_kw_hard staat los
    van max_afname_kw_hard. Default door server.js = som fysieke inverters
    (pv.inverter_kw + batterij.kw). Asymmetrie afname↔injectie (Belgisch
    tarief: Groep B / D wegen op afname-piek, Groep C ≈ € 0 voor MS).
  - Status-check aangescherpt: ALLEEN 'Optimal' is acceptabel. 'Not Solved'
    en 'Infeasible' triggert nu warning + (in BSP) retry-ladder.
  - Post-solve validatie + clip: max(grid_in_vals) > cap → warning + clip op cap.
  - eps_penalty (1 €/MWh) op (grid_in + grid_out) toegevoegd aan lp_dispatch_day
    en lp_dispatch_day_stacked (was alleen in BSP). Anti-fake-volume.

  ARCHITECTUUR (BSP-retry) — lp_dispatch_day_bsp:
  - Interne helper _build_and_solve_bsp(spec_budget_multiplier) extract.
  - Retry-ladder bij niet-Optimal status:
      Niveau 1: spec_budget × 10  (verwachte oplos-rate ~95%)
      Niveau 2: spec_budget volledig verwijderen (~4% extra)
      Niveau 3: GEEN passieve fallback. Dag wordt overgeslagen, grid_in/out
                = 0 voor 96 kwartieren. SoC blijft op begin-waarde.
    Reden: passieve dispatch zou business-case kunstmatig optimistisch maken
    want werkelijk zou Fluctus op zo'n moment ook geen BSP-flex toepassen.

  FACTUUR — bereken_jaarfactuur:
  - Output gesplitst: jaarpiek_afname_kw + jaarpiek_injectie_kw als
    expliciete velden. toegangsvermogen_kw blijft als backwards-compat alias
    voor jaarpiek_afname_kw. winterpiek_afname_kw nieuw veld bewaart de
    v1.5-semantiek voor Elia transport-jaarpiek.
  - SEMANTIEK-WIJZIGING: jaarpiek_afname_kw was in v1.5 de WINTER-piek
    (nov-mrt, voor Elia transport). In v1.6 = max(maandpieken_afname).
    De Elia-transportberekening gebruikt nu winterpiek_afname_kw intern,
    zodat factuurbedragen IDENTIEK blijven.
  - Nieuwe parameter aansluiting (optioneel): wanneer meegegeven worden
    maandpieken > aansluiting in een aparte overschrijdings-bucket gerekend
    (twee-bucket Groep B). Bij None (default): v1.5-gedrag behouden.

  HOOFDLUS — run_simulation:
  - Vervangen: heeft_batterij switch (regel ~1837) door heeft_lp_output.
    Reden: bij PV-only + BSP zonder BESS draait de LP wel maar werd de
    LP-output genegeerd en passief gereconstrueerd (= pv_curt vergeten).
  - SoC-cascade sanity check: bij ongeldige soc[-1] (None / < 0 / > soc_max),
    log warning en reset naar 0.5 × soc_max.
  - lp_diagnostics: nieuwe output-velden totaal_dagen / optimal_dagen /
    retry1_dagen / retry2_dagen / verloren_dagen (lijst datums).
  - Aansluiting doorgegeven naar bereken_jaarfactuur.

  CLEAN-UP:
  - Alle [DIAG-1] t/m [DIAG-8c] log-blokken uit v1.5.1-diag en v1.5.2-diag
    verwijderd (waren tijdelijk voor RCA, niet voor productie).

Anti-regressie:
  - Sc1-3 (skip_lp / geen PV-injectie): identieke output aan v1.5.
  - Backwards-compat: factuur['toegangsvermogen_kw'] blijft = afname-jaarpiek.
  - Bestaande bewaarde scenarios laden zonder migratie.

Wijzigingen v1.5 (BaseCase Uitbreiding sessie 4 — periode-handling):
  - Nieuwe simulatieperiode-modus "specifiek": simulatieperiode.type='specifiek'
    laat de simulator exact de meegegeven [van, tot) periode draaien zonder
    extrapolatie naar volledig jaar. Bedoeld voor base-case-factuurvergelijking
    (bv. Smartunit januari 2026 over die exacte maand simuleren).
  - Bij type='specifiek' wordt 'jaarverbruik_mwh' geinterpreteerd als
    PERIODE-volume (= factuur-afname over [van, tot)), NIET als jaarvolume.
    Simulator berekent intern het effectief jaarverbruik via de profielfractie
    in periode (zelfde weekdag-bewuste mapping als project_jaarverbruik.py).
  - Effectief jaarverbruik wordt gebruikt voor Enwyse-staffel-tier en
    accijnzen-staffel-keuze (zodat klant niet in te lage schijf valt als
    periode-volume <<< jaarvolume).
  - Vaste-kost componenten (vaste_kost_leverancier, databeheer afname/injectie,
    vaste_vergoeding injectie, beschikbaar_vermogen, energiefonds) worden
    geprorateerd op n_kalendermaanden_in_periode / 12. Voor volledige jaren
    (12 maanden) verandert er niets — bewezen 0-regressie via diff.
  - Anti-regressie: alle wijzigingen aan bereken_jaarfactuur zijn additief via
    nieuwe default-parameters (effectief_jaarverbruik_voor_pricing=None,
    n_maanden=12 default). Calls zonder die parameters geven IDENTIEKE output.
  - Bestaande paden rolling12/kalenderjaar zijn ongewijzigd: type-veld optional,
    default = oude gedrag.

Wijzigingen v1.4:
  - BSP-modus (Niveau 3b): perfect IMB-foresight LP met:
    * forecast-met-ruis als nominatie (Optie E)
    * fysieke flex (PV-curtail, batterij) als BSP-edge
    * paper capture rate (gekalibreerd op 1.8% tegen externe simulator)
    * forecast_modus: conservatief / realistic / optimistisch (× 0.67/1.0/1.5)
  - KRITIEKE BUG FIX: passieve passthrough imbalance regende altijd op
    (∑ vol × (IMB-DAM)). Dat hoort 0 te zijn voor passive klanten.
    Live productie had hier een systematische fout.
  - Refactored bereken_jaarfactuur met optionele nom_afn_kw_all/nom_inj_kw_all
    voor BSP-decompositie. Backwards-compat met oude calls.
  - Input: bsp = { actief: bool, paper_capture_rate: float, 
                   forecast_modus: 'conservatief'|'realistic'|'optimistisch',
                   pv_curtailment_allowed: bool }

Wijzigingen v1.3:
  - PV-curtailment optie: cap PV op eigen verbruik wanneer DAM < drempel
    Input: pv_curtailment = { actief: bool, trigger_eur_mwh: float, strategie: 'cap_op_verbruik' }
  - Nieuwe KPI's: pv_curtailed_mwh, pv_curtailed_kwartieren,
                  pv_potentiele_productie_mwh, vermeden_injectie_kost_eur

Wijzigingen v1.2:
  - PV-KPI labels gecorrigeerd:
    * pct_zelfconsumptie  = pv_eigen / pv_totaal × 100   (was verwisseld)
    * pct_zelfvoorziening = pv_eigen / verbruik × 100    (was verwisseld)
  - Nieuwe expliciete velden: pv_eigen_verbruik_mwh, pv_injectie_mwh

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
  - simulatieperiode: { van: "YYYY-MM-DD", tot: "YYYY-MM-DD",
                        type: "kalenderjaar" | "specifiek" (optional, default: kalenderjaar) }
                        # NIEUW v1.5: bij type="specifiek" wordt jaarverbruik_mwh
                        # geinterpreteerd als periode-volume (factuur-afname),
                        # niet als jaarvolume. Vaste-kost componenten worden
                        # geprorateerd op kalendermaanden-fractie.
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


# ─── Helpers voor periode-handling (v1.5 sessie 4) ──────────────────────────

def _n_kalendermaanden_in_periode(sim_timestamps: list) -> int:
    """
    Tel het aantal unieke (jaar, maand) tuples in sim_timestamps.
    Gebruikt voor prorata van vaste-kost componenten in bereken_jaarfactuur.
    
    Voorbeelden:
      - jan 2026 (2026-01-01 → 2026-02-01, exclusief tot): {(2026,1)} = 1
      - dec 2025 (2025-12-01 → 2026-01-01): {(2025,12)} = 1
      - jaar 2025 (2025-01-01 → 2026-01-01): 12 unieke tuples = 12
      - rolling12 apr→apr (2025-04-28 → 2026-04-27): typisch 13 (4 unieke maanden in 2025 + maanden in 2026)
    
    Voor het defaultpad (volledig jaar of rolling) blijft de caller op
    n_maanden=12 hardcoded en raakt deze helper niet — alleen het
    type="specifiek" pad gebruikt deze functie.
    """
    if not sim_timestamps:
        return 0
    seen = set()
    for ts in sim_timestamps:
        seen.add((ts.year, ts.month))
    return len(seen)


def _profielfractie_in_periode(basis_profiel: list, sim_timestamps: list) -> float:
    """
    Bereken de som van basis_profiel-waarden voor de kwartieren in
    sim_timestamps, via WEEKDAG-BEWUSTE 2025-mapping (zelfde radius-3
    logica als project_jaarverbruik.py).
    
    Gebruikt quarter_index_in_year_2025() — single source of truth voor
    mapping. Returnt typisch < 1.0 (basis_profiel heeft som=1.0 over heel
    jaar). Voor jan 2026 op een kantoorprofiel: ~0.085 (ongeveer 1/12,
    licht aangepast voor seizoenseffecten en weekdag-mapping).
    
    Wordt gebruikt om effectief_jaarverbruik te berekenen:
      effectief_jaarverbruik_mwh = periode_volume_mwh / profielfractie
    """
    if not basis_profiel or not sim_timestamps:
        return 0.0
    if len(basis_profiel) != 35040:
        log.warning(
            f"_profielfractie_in_periode: basis_profiel heeft {len(basis_profiel)} "
            f"waarden, verwacht 35040. Resultaat onbetrouwbaar."
        )
    totaal = 0.0
    for ts in sim_timestamps:
        idx = quarter_index_in_year_2025(ts)
        if 0 <= idx < len(basis_profiel):
            totaal += basis_profiel[idx]
    return totaal


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
# LAADPLEINEN — flexibele EV-laadvraag (v1.8)
# =============================================================================
# Model (Johan, 12/07/2026):
#   - Elk laadplein = 1 voertuigtype: aantal wagens × km/jaar × kWh/km = jaarenergie.
#   - Sessies binnen een venster (start-eind uur) op 5/6/7 dagen/week
#     (5=weekdagen, 6=+zaterdag, 7=+zondag). Dagenergie = jaarenergie / (dagen/week×52),
#     te leveren binnen het venster op elke laaddag.
#   - Vermogenscap = som laadpunten (AC22=22, DC160=160, DC300=300 kW).
#   - Sturing = 3 varianten: 'onmiddellijk' (variant 1, dom laden vanaf vensterstart),
#     'shift_spot' (variant 2: eerst PV-zelfconsumptie, dan goedkoopste spot-uren),
#     'shift_onbalans' (variant 3: idem maar prijs = min(spot, onbalans) → extra laden
#     bij zeer lage/negatieve onbalansprijs).
#   - Bestaand laadplein: energie zit al in het factuur-jaarverbruik → we schalen die
#     eruit en voegen het als een gewoon (stuurbaar) laadplein toe.

_LAADPUNT_KW = {'AC22': 22.0, 'DC160': 160.0, 'DC300': 300.0}


def _is_laaddag(dt, dpw: int) -> bool:
    wd = dt.weekday()  # ma=0 .. zo=6
    if dpw >= 7:
        return True
    if dpw == 6:
        return wd <= 5   # ma-za
    return wd <= 4       # ma-vr


def _laadplein_prep(inp: dict, sim_timestamps: list) -> dict:
    """Normaliseer de laadpleinen + bereken per plein cap_kw, dagenergie en de
    energie over de sim-periode (via het aantal laaddagen in de periode)."""
    pleinen_in = inp.get('laadpleinen') or []
    out = []
    tot_cap_kw = 0.0
    bestaand_periode_kwh = 0.0
    nieuw_periode_kwh = 0.0
    # Laaddagen per dagen/week-instelling tellen we één keer.
    datums = {}
    for ts in sim_timestamps:
        datums.setdefault(ts.date(), ts)
    def _n_laaddagen(dpw):
        return sum(1 for d, ts in datums.items() if _is_laaddag(ts, dpw))

    for p in pleinen_in:
        aantal = float(p.get('aantal', 0) or 0)
        kmj    = float(p.get('km_per_jaar', 0) or 0)
        kwh_km = float(p.get('kwh_per_km', 0) or 0)
        jaar_kwh = aantal * kmj * kwh_km
        dpw = int(p.get('dagen_per_week', 5) or 5)
        if dpw not in (5, 6, 7):
            dpw = 5
        v_start = float(p.get('venster_start', 8) or 0)
        v_eind  = float(p.get('venster_eind', 18) or 24)
        # v1.8.9: overnacht-/wrap-around-venster (v_eind <= v_start, bv. 18→8u)
        # wordt nu ondersteund. De sessie loopt van v_start op dag D tot v_eind
        # op dag D+1. venster_uren telt beide stukken samen.
        _wrap = v_eind <= v_start
        cap = 0.0
        for lp in (p.get('laadpunten') or []):
            cap += float(lp.get('aantal', 0) or 0) * _LAADPUNT_KW.get(lp.get('type'), 0.0)
        if p.get('cap_kw'):
            cap = float(p['cap_kw'])
        laaddagen_jaar = dpw * 52
        dag_kwh = jaar_kwh / laaddagen_jaar if laaddagen_jaar > 0 else 0.0
        # Als geen laadpunten opgegeven: default cap zodat de dagenergie in het
        # venster past (spreiding), zodat de sim niet vastloopt.
        venster_uren = max(0.25, ((24.0 - v_start) + v_eind) if _wrap else (v_eind - v_start))
        if cap <= 0 and dag_kwh > 0:
            cap = dag_kwh / venster_uren
        op_dagen = _n_laaddagen(dpw)
        periode_kwh = dag_kwh * op_dagen
        rec = {
            'naam': p.get('naam', 'laadplein'), 'aantal': aantal,
            'jaar_kwh': jaar_kwh, 'dag_kwh': dag_kwh, 'cap_kw': cap,
            'v_start': v_start, 'v_eind': v_eind, 'wrap': _wrap, 'dpw': dpw,
            'bestaand': bool(p.get('bestaand')), 'periode_kwh': periode_kwh,
        }
        out.append(rec)
        tot_cap_kw += cap
        if rec['bestaand']:
            bestaand_periode_kwh += periode_kwh
        else:
            nieuw_periode_kwh += periode_kwh
    return {
        'pleinen': out, 'tot_cap_kw': tot_cap_kw,
        'bestaand_periode_mwh': bestaand_periode_kwh / 1000.0,
        'nieuw_periode_mwh': nieuw_periode_kwh / 1000.0,
        'totaal_periode_mwh': (bestaand_periode_kwh + nieuw_periode_kwh) / 1000.0,
    }


def _bouw_ev_load(lp_prep: dict, sim_timestamps: list, spot: list, imb: list,
                  pv_kw: list, base_cons_kw: list, modus: str,
                  connection_kw: float = 1e12, battery_kw: float = 0.0):
    """Bouw het EV-laadprofiel (kW/kwartier) voor alle laadpleinen samen, CONNECTION-AWARE:
    per kwartier laden we max = min(laadpunt-cap, vrije ruimte onder het toegangsvermogen
    (+ batterij-buffer)). We zetten dus nooit 'ineens alle vermogen' aan.
    Returnt (ev_load, tekort_kwh): tekort = dagenergie die NIET geladen raakte binnen het
    venster onder de aansluiting (drijft de dimensionering van verhoging/batterij).
    Modi: 'onmiddellijk' (variant 1, chronologisch vullen), 'shift_spot' (PV-zelfconsumptie
    dan goedkoopste spot), 'shift_onbalans' (idem maar prijs = min(spot, onbalans))."""
    import datetime as _dt
    N = len(sim_timestamps)
    ev = [0.0] * N
    dt_h = 0.25
    tekort_kwh = 0.0
    per_dag = {}
    for i, ts in enumerate(sim_timestamps):
        per_dag.setdefault(ts.date(), []).append(i)

    def _uur(i):
        return sim_timestamps[i].hour + sim_timestamps[i].minute / 60.0

    def _headroom_kw(i):
        # Vrije ruimte onder de aansluiting (+ batterijbuffer), na wat al toegewezen is.
        return max(0.0, connection_kw + battery_kw - base_cons_kw[i] - ev[i])

    for rec in lp_prep['pleinen']:
        if rec['dag_kwh'] <= 0 or rec['cap_kw'] <= 0:
            continue
        cap, vs, ve, dpw = rec['cap_kw'], rec['v_start'], rec['v_eind'], rec['dpw']
        wrap = rec.get('wrap', False)
        for dag, idxs in per_dag.items():
            if not _is_laaddag(sim_timestamps[idxs[0]], dpw):
                continue
            if not wrap:
                # Gewoon dagvenster: vs <= uur < ve op dezelfde dag.
                venster = [i for i in idxs if vs <= _uur(i) < ve]
            else:
                # v1.8.9 overnacht/wrap-around: sessie start op dag D vanaf vs en
                # loopt door tot ve op dag D+1. Avonddeel (dag D, uur >= vs) +
                # ochtenddeel (dag D+1, uur < ve), chronologisch gesorteerd.
                _dnext = dag + _dt.timedelta(days=1)
                if _dnext not in per_dag:
                    # Incomplete rand-sessie aan het einde van de periode (geen
                    # ochtend erna) → overslaan, anders drijft die afgekapte sessie
                    # de dimensionering kunstmatig op.
                    continue
                venster = [i for i in idxs if _uur(i) >= vs]
                venster += [i for i in per_dag[_dnext] if _uur(i) < ve]
                venster.sort()
            if not venster:
                continue
            if modus == 'onmiddellijk':
                volgorde = list(venster)  # chronologisch
            else:
                surplus = dict((i, max(0.0, pv_kw[i] - base_cons_kw[i])) for i in venster)
                def _prijs(j):
                    return min(spot[j], imb[j]) if modus == 'shift_onbalans' else spot[j]
                # PV-overschot-kwartieren eerst (zelfconsumptie), dan goedkoopste prijs.
                volgorde = sorted(venster, key=lambda j: (0 if surplus[j] > 0 else 1, _prijs(j)))
            resterend = rec['dag_kwh']
            for i in volgorde:
                if resterend <= 1e-9:
                    break
                beschikbaar_kw = min(cap, _headroom_kw(i))
                take = min(beschikbaar_kw * dt_h, resterend)
                if take > 0:
                    ev[i] += take / dt_h
                    resterend -= take
            if resterend > 1e-6:
                tekort_kwh += resterend  # paste niet onder de aansluiting in dit venster
    return ev, tekort_kwh


def _laadplein_capaciteit(lp_prep: dict, sim_timestamps: list, spot: list, imb: list,
                          pv_kw: list, base_cons_kw: list, toegangsvermogen: float) -> dict:
    """Capaciteits-check + dimensionering (v1.8):
      - Past de EV-laadvraag binnen het venster onder het huidige toegangsvermogen?
      - Zo niet: (opstelling 1) minimale verhoging van het toegangsvermogen om alle energie
        er tijdens de sessie door te krijgen; (opstelling 2) minimale batterij (kW/kWh) om
        binnen het huidige toegangsvermogen te blijven."""
    leeg = {'voldoende': True, 'tekort_mwh': 0.0,
            'huidig_toegangsvermogen_kw': round(toegangsvermogen, 1),
            'benodigd_toegangsvermogen_kw': round(toegangsvermogen, 1), 'verhoging_kw': 0.0,
            'advies_batterij_kw': 0.0, 'advies_batterij_kwh': 0.0}
    if not lp_prep['pleinen'] or lp_prep['totaal_periode_mwh'] <= 0:
        return leeg

    # v1.8.8: dimensioneer op SLIM laden (shift_spot = variant 2), i.p.v. op dom
    # laden. Zo matcht de geadviseerde batterij hoe hij in de standaardsturing
    # (variant 2) echt draait, en vermijden we een mismatch die anders de
    # feasibility-terugval uitlokt.
    _DIM_MODUS = 'shift_spot'

    def _tekort(P, batt):
        _, t = _bouw_ev_load(lp_prep, sim_timestamps, spot, imb, pv_kw, base_cons_kw,
                             _DIM_MODUS, connection_kw=P, battery_kw=batt)
        return t

    short0 = _tekort(toegangsvermogen, 0.0)
    if short0 <= 1e-6:
        return leeg

    _bovengrens = toegangsvermogen + lp_prep['tot_cap_kw'] + (max(base_cons_kw) if base_cons_kw else 0) + 1.0
    # Opstelling 1 — minimale verhoging toegangsvermogen (geen batterij).
    lo, hi = toegangsvermogen, _bovengrens
    for _ in range(28):
        mid = (lo + hi) / 2.0
        if _tekort(mid, 0.0) <= 1e-6:
            hi = mid
        else:
            lo = mid
    verhoogd_P = hi
    # Opstelling 2 — minimale batterij-kW bij het huidige toegangsvermogen.
    lo, hi = 0.0, _bovengrens
    for _ in range(28):
        mid = (lo + hi) / 2.0
        if _tekort(toegangsvermogen, mid) <= 1e-6:
            hi = mid
        else:
            lo = mid
    _p_min = hi   # minimaal batterijVERMOGEN voor haalbaarheid (piek boven aansluiting)
    # Batterij-kWh = zwaarste boven-aansluiting-energie in een ROLLEND 24u-venster
    # (met DoD-marge). v1.8.9: rollend i.p.v. per kalenderdag, zodat een overnacht-
    # sessie (avond + ochtend over middernacht) niet gesplitst en onderschat wordt.
    evb, _ = _bouw_ev_load(lp_prep, sim_timestamps, spot, imb, pv_kw, base_cons_kw,
                           _DIM_MODUS, connection_kw=toegangsvermogen, battery_kw=_p_min)
    _boven = [max(0.0, (base_cons_kw[i] + evb[i]) - toegangsvermogen) * 0.25
              for i in range(len(sim_timestamps))]
    _W = 96  # 24u = 96 kwartieren
    _run = sum(_boven[:_W])
    max_dag_kwh = _run
    for i in range(_W, len(_boven)):
        _run += _boven[i] - _boven[i - _W]
        if _run > max_dag_kwh:
            max_dag_kwh = _run
    _e_cap = max_dag_kwh / 0.90   # nodige capaciteit uit dagenergie (1 cyclus, DoD-marge)
    # v1.8.10 — 2-UURS BATTERIJ (Johan): standaardcomponenten. Capaciteit = dagenergie,
    # vermogen = capaciteit / 2. Het laadpunt-/haalbaarheids-minimumvermogen (_p_min)
    # is de vloer: ligt dat hoger dan capaciteit/2, dan groeit de batterij mee
    # (kWh = 2 × kW) zodat het een geldige 2u-batterij blijft.
    _bat_kw = max(_e_cap / 2.0, _p_min)
    _bat_kwh = 2.0 * _bat_kw
    return {
        'voldoende': False, 'tekort_mwh': round(short0 / 1000.0, 3),
        'huidig_toegangsvermogen_kw': round(toegangsvermogen, 1),
        'benodigd_toegangsvermogen_kw': round(verhoogd_P, 1),
        'verhoging_kw': round(verhoogd_P - toegangsvermogen, 1),
        'advies_batterij_kw': round(_bat_kw, 1),
        'advies_batterij_kwh': round(_bat_kwh, 1),
        # diagnostiek: energie-gedreven capaciteit + vermogen-vloer apart
        'dagenergie_boven_aansluiting_kwh': round(_e_cap, 1),
        'min_vermogen_kw': round(_p_min, 1),
    }


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

def _natuurlijke_lastpiek_kw(consumption_kw: list, pv_kw: list) -> float:
    """v1.8.4: piek van de NATUURLIJKE afname (verbruik − PV, ondergrens 0) over
    de periode. De batterij/sturing mag grid_in nooit boven deze piek duwen: zo
    loopt de (zekere) maandpiek niet op om (onzekere) arbitrage/onbalans te
    oogsten. Piekshaving ONDER deze grens blijft toegelaten (en beloond)."""
    H = len(consumption_kw)
    if H == 0:
        return 0.0
    piek = 0.0
    for t in range(H):
        pv = pv_kw[t] if pv_kw and t < len(pv_kw) else 0.0
        netto = consumption_kw[t] - pv
        if netto > piek:
            piek = netto
    return max(0.0, piek)


def _gin_cap_normaal(consumption_kw: list, pv_kw: list, max_afname_hard: float) -> float:
    """grid_in-bovengrens in normale modus = min(contract, natuurlijke lastpiek).
    Bij verwaarloosbare last (< 0,5 kW) niet begrenzen (val terug op contract)."""
    p_nat = _natuurlijke_lastpiek_kw(consumption_kw, pv_kw)
    if p_nat <= 0.5:
        return max_afname_hard
    return min(max_afname_hard, math.ceil(p_nat))


def lp_dispatch_day(
    consumption_kw: list,         # 96 kwartieren
    pv_kw: list,                  # 96 kwartieren
    spot_eur_mwh: list,           # 96 kwartieren (forecast)
    soc_start_kwh: float,         # SoC bij start van de dag
    batterij: dict,               # kw, kwh, dod_pct, rte_pct, capex, max_cycli
    aansluiting: dict,
    contract: dict,
    cyclus_kost_eur_per_kwh: float,
    imb_eur_mwh: list = None,     # v1.5 Stacked: IMB-prijs voor dispatch (None = gebruik spot)
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

    # v1.6: Tegen-tarieven voor de ZACHTE overschrijdings-band. Hard cap zit nu
    # in de LpVariable upper bound (geen soft hard-penalty meer, want dat creëerde
    # silent failure-mode bij infeasibility — zie RCA defect 3).
    tar_afname_kw = aansluiting.get('tarief_overschrijding_afname_eur_per_kw_jaar', 62.47)
    tar_injectie_kw = aansluiting.get('tarief_overschrijding_injectie_eur_per_kw_jaar', 1.0)

    pen_afname_zacht = tar_afname_kw / 12.0   # €/kW per overschrijdingskwartier (benadering)
    pen_injectie_zacht = tar_injectie_kw / 12.0

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

    # v1.5 Stacked: gebruik IMB-prijs voor dispatch als meegegeven
    dispatch_prijs = imb_eur_mwh if imb_eur_mwh is not None else spot_eur_mwh

    # LP setup
    prob = pulp.LpProblem('battery_dispatch', pulp.LpMinimize)

    # v1.8.4: grid_in nooit boven de natuurlijke lastpiek (geen piek-inflatie
    # door arbitrage). Piekshaving eronder blijft mogelijk.
    _gin_cap = _gin_cap_normaal(consumption_kw, pv_kw, max_afname_hard)

    p_ch = [pulp.LpVariable(f'pch_{t}', 0, kw_batt) for t in range(H)]
    p_dis = [pulp.LpVariable(f'pdis_{t}', 0, kw_batt) for t in range(H)]
    grid_in = [pulp.LpVariable(f'gin_{t}', 0, _gin_cap) for t in range(H)]
    grid_out = [pulp.LpVariable(f'gout_{t}', 0, max_injectie_hard) for t in range(H)]
    soc = [pulp.LpVariable(f'soc_{t}', soc_min, soc_max) for t in range(H + 1)]
    # v1.6: alleen 'zacht' overschrijdings-tracking. De hard cap zit nu in de
    # LpVariable upper bound — geen aparte over_*_hard variabele meer.
    over_afn_zacht = [pulp.LpVariable(f'oaz_{t}', 0) for t in range(H)]
    over_inj_zacht = [pulp.LpVariable(f'oiz_{t}', 0) for t in range(H)]

    # Initiële SoC
    prob += soc[0] == soc_start_kwh

    # Power balance per kwartier:
    # consumption = pv + grid_in - grid_out + p_dis - p_ch
    for t in range(H):
        prob += grid_in[t] - grid_out[t] + p_dis[t] - p_ch[t] + pv_kw[t] == consumption_kw[t]
        # SoC update
        prob += soc[t + 1] == soc[t] + eta * p_ch[t] * dt_h - (1.0 / eta) * p_dis[t] * dt_h
        # Zachte overschrijdings-tracking (lineaire activatie):
        #   over_afn_zacht = max(0, grid_in - max_zacht)
        # De LP minimaliseert dit via objective. De HARDE bound zit in de
        # LpVariable upper bound (regel boven) — geen dubbele bound-mechanismen
        # zoals in v1.5 (zie RCA defect 3).
        prob += over_afn_zacht[t] >= grid_in[t] - max_afname_zacht
        prob += over_inj_zacht[t] >= grid_out[t] - max_injectie_zacht
        # Cosφ-constraint (gebundeld): laden + ontladen + grid niet allemaal tegelijk op max
        # Vereenvoudiging: omvormer-cap p_ch + p_dis ≤ kw_batt (mutually exclusive in praktijk)
        prob += p_ch[t] + p_dis[t] <= kw_batt

    # Objective: minimaliseer kost
    obj_terms = []
    # v1.6: anti-fake-volume eps op (grid_in + grid_out). Voorkomt dat LP
    # simultaan op de hard cap zit op beide richtingen alleen om penalties
    # rond te schuiven. 1 €/MWh is significant maar economisch verwaarloosbaar.
    eps_penalty = 1.0 / 1000.0
    for t in range(H):
        # Energiekost afname (€/kwartier): kW * 0.25 * (€/MWh) / 1000
        # v1.1: vergroening (€/MWh) wordt opgeteld bij afnameprijs.
        # Geen floor op injectieprijs — kan negatief zijn bij negatieve spot.
        prijs_afn_t = (dispatch_prijs[t] + markup_per_mwh + gsc + wkk + vergroening) / 1000.0
        prijs_inj_t = (dispatch_prijs[t] - markdown_per_mwh) / 1000.0
        obj_terms.append(prijs_afn_t * grid_in[t] * dt_h)
        obj_terms.append(-prijs_inj_t * grid_out[t] * dt_h)
        # Cyclus-kost: ontladen kost
        obj_terms.append(cyclus_kost_eur_per_kwh * p_dis[t] * dt_h)
        # Zachte overschrijdings-penalties (alleen op het deel boven het
        # zachte plafond — het deel boven het harde plafond kan in v1.6 niet
        # meer voorkomen want hard cap is variabele-bound).
        obj_terms.append(pen_afname_zacht * over_afn_zacht[t])
        obj_terms.append(pen_injectie_zacht * over_inj_zacht[t])
        # Anti-fake-volume eps
        obj_terms.append(eps_penalty * (grid_in[t] + grid_out[t]) * dt_h)

    prob += pulp.lpSum(obj_terms)

    solver = pulp.PULP_CBC_CMD(msg=0, timeLimit=10)
    status = prob.solve(solver)

    # v1.6: alleen Optimal is acceptabel. Bij Infeasible/Not Solved geeft CBC
    # soms tóch waarden voor variabelen die de bounds overschrijden — daarom
    # post-solve validatie + clip (anti silent-failure).
    status_str = pulp.LpStatus[status]
    if status_str != 'Optimal':
        log.warning(f"LP-status non-optimal: {status_str} — output kan onnauwkeurig zijn")

    # Extract values
    p_ch_vals = [pulp.value(v) or 0.0 for v in p_ch]
    p_dis_vals = [pulp.value(v) or 0.0 for v in p_dis]
    grid_in_vals = [pulp.value(v) or 0.0 for v in grid_in]
    grid_out_vals = [pulp.value(v) or 0.0 for v in grid_out]
    soc_vals = [pulp.value(v) or 0.0 for v in soc]

    # Post-solve clip (laatste defensie tegen CBC bound-violations).
    _tol = 0.01
    if grid_in_vals and max(grid_in_vals) > max_afname_hard + _tol:
        log.warning(
            f"LP grid_in bound-violation: max={max(grid_in_vals):.2f} > cap={max_afname_hard:.2f} — clip toegepast"
        )
        grid_in_vals = [min(v, max_afname_hard) for v in grid_in_vals]
    if grid_out_vals and max(grid_out_vals) > max_injectie_hard + _tol:
        log.warning(
            f"LP grid_out bound-violation: max={max(grid_out_vals):.2f} > cap={max_injectie_hard:.2f} — clip toegepast"
        )
        grid_out_vals = [min(v, max_injectie_hard) for v in grid_out_vals]
    if p_ch_vals and max(p_ch_vals) > kw_batt + _tol:
        log.warning(f"LP p_ch bound-violation: max={max(p_ch_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_ch_vals = [min(v, kw_batt) for v in p_ch_vals]
    if p_dis_vals and max(p_dis_vals) > kw_batt + _tol:
        log.warning(f"LP p_dis bound-violation: max={max(p_dis_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_dis_vals = [min(v, kw_batt) for v in p_dis_vals]

    return {
        'p_charge': p_ch_vals,
        'p_discharge': p_dis_vals,
        'grid_in': grid_in_vals,
        'grid_out': grid_out_vals,
        'soc': soc_vals,
        'over_afn_zacht': [pulp.value(v) or 0.0 for v in over_afn_zacht],
        'over_inj_zacht': [pulp.value(v) or 0.0 for v in over_inj_zacht],
        # v1.6: backwards-compat — over_*_hard worden nu altijd 0 geleverd
        # zodat callers die deze keys lazen niet breken. Het hard-overschrijden
        # van de cap zou in v1.6 niet meer mogelijk moeten zijn na clip.
        'over_afn_hard': [0.0] * H,
        'over_inj_hard': [0.0] * H,
        'lp_status': status_str,
    }


def lp_dispatch_day_bsp(
    consumption_kw: list,         # 96 kwartieren — werkelijk verbruik
    pv_kw: list,                  # 96 kwartieren — werkelijk PV (potentieel, vóór curtailment)
    spot_eur_mwh: list,           # 96 kwartieren — DAM (D-1 bekend)
    imb_eur_mwh: list,            # 96 kwartieren — IMB werkelijk (perfect foresight!)
    soc_start_kwh: float,
    batterij: dict,
    aansluiting: dict,
    contract: dict,
    cyclus_kost_eur_per_kwh: float,
    daily_paper_risk_mwh: float,  # max ∑(dev_pos+dev_neg) × dt / 1000 per dag in MWh
    pv_curtailment_allowed: bool, # of LP PV mag curtailen (anders pv_curtail=0)
    consumption_forecast_kw: list = None,  # v1.4: forecast (= nominatie) — None=geen ruis
    pv_forecast_kw: list = None,           # v1.4: forecast (= nominatie) — None=geen ruis
) -> dict:
    """
    BSP-uitbreiding van lp_dispatch_day. Realistisch model:
      - Nominatie = verwachte werkelijke positie (vast, op basis van consumption-pv).
        BSP heeft GEEN vrijheid om te kunstmatig te ondernomineren of overnomineren.
      - LP beslist over:
        1. PV curtailment (fysieke flexibiliteit)
        2. Batterij dispatch (fysieke flexibiliteit)
        3. Papier-deviation: dev_pos/dev_neg t.o.v. nominatie, binnen risk-budget.
           Dit is een financiële swap: deel van volume wordt via IMB afgerekend ipv DAM.

    Realisatie balans:
      grid_in[t] - grid_out[t]
        = consumption[t] - (pv[t] - pv_curtail[t]) + p_ch[t] - p_dis[t]   (fysiek)
        = nom_net[t] + (- pv_curtail[t] + p_ch[t] - p_dis[t]) + (dev_pos[t] - dev_neg[t])
      waarbij nom_net[t] = consumption[t] - pv[t]  (vast)

    DAM-tak rekent op nom_net (afgerekend tegen DAM met markup/markdown).
    IMB-tak rekent op (fysieke-flex + papier-dev) tegen IMB (geen markup).

    v1.6: retry-ladder bij niet-Optimal status. Drie niveaus, geen passieve
    fallback (zie RCA defect 3 + design beslissing 4.3).

    Returns: zelfde keys als lp_dispatch_day + BSP-extra:
      - nom_dam_kw[96]: vaste nominatie (afname-positief)
      - dev_kw[96]: dev_pos - dev_neg
      - pv_curtailed_kw[96]
      - nom_revenue_eur, dev_revenue_eur
      - lp_status: 'Optimal' / 'retry1_optimal' / 'retry2_optimal' / 'verloren'
      - retry_level: 0 / 1 / 2 / 3 (3 = verloren)
    """
    H = 96
    dt_h = 0.25

    kw_batt = batterij['kw']
    kwh_batt = batterij['kwh']
    dod = batterij['dod_pct'] / 100.0 if batterij['dod_pct'] > 1.5 else batterij['dod_pct']
    rte = batterij['rte_pct'] / 100.0 if batterij['rte_pct'] > 1.5 else batterij['rte_pct']
    eta = math.sqrt(rte)

    soc_min = 0.0
    soc_max = kwh_batt * dod

    # v1.6: alleen ZACHTE overschrijdings-penalties. Harde bounds zitten in
    # LpVariable upper bounds — geen aparte over_*_hard variabelen meer (RCA defect 3).
    tar_afname_kw = aansluiting.get('tarief_overschrijding_afname_eur_per_kw_jaar', 62.47)
    tar_injectie_kw = aansluiting.get('tarief_overschrijding_injectie_eur_per_kw_jaar', 1.0)
    pen_afname_zacht = tar_afname_kw / 12.0
    pen_injectie_zacht = tar_injectie_kw / 12.0

    max_afname_zacht = aansluiting.get('max_afname_kw_zacht', 1e9)
    max_afname_hard = aansluiting.get('max_afname_kw_hard', 1e9)
    max_injectie_zacht = aansluiting.get('max_injectie_kw_zacht', 1e9)
    max_injectie_hard = aansluiting.get('max_injectie_kw_hard', 1e9)

    injectie_toegelaten = contract.get('injectie_toegelaten', True)
    if not injectie_toegelaten:
        max_injectie_zacht = 0.0
        max_injectie_hard = 0.0

    jaarverbruik_voor_pricing = contract.get('jaarverbruik_mwh', 0.0)
    if not jaarverbruik_voor_pricing:
        jaarverbruik_voor_pricing = sum(consumption_kw) * 0.25 * 365 / 1000.0
    pricing = resolve_contract_pricing(contract, jaarverbruik_voor_pricing)

    markup_per_mwh = pricing['markup_dam']
    markdown_per_mwh = pricing['markdown_dam']
    vergroening = pricing['vergroening']
    gsc = pricing['gsc']
    wkk = pricing['wkk']

    # ── NOMINATIE = forecast met ruis (referentie voor speculation budget) ──
    cons_for_nom = consumption_forecast_kw if consumption_forecast_kw is not None else consumption_kw
    pv_for_nom = pv_forecast_kw if pv_forecast_kw is not None else pv_kw
    forecast_afn = [max(cons_for_nom[t] - pv_for_nom[t], 0) for t in range(H)]
    forecast_inj = [max(pv_for_nom[t] - cons_for_nom[t], 0) for t in range(H)]
    nom_net = [cons_for_nom[t] - pv_for_nom[t] for t in range(H)]  # behouden voor return

    def _build_and_solve_bsp(spec_budget_multiplier: float, label: str):
        """
        v1.6 retry-helper: bouwt en solveert het BSP-LP met een gegeven
        spec_budget_multiplier toegepast op daily_paper_risk_mwh.

        multiplier=1.0  → normale spec_budget (eerste poging)
        multiplier=10.0 → relaxed spec_budget (retry niveau 1)
        multiplier=-1   → spec_budget volledig verwijderd (retry niveau 2)

        Returns tuple (status_str, result_dict) waar result_dict alle
        relevante arrays bevat.
        """
        prob = pulp.LpProblem(f'battery_dispatch_bsp_{label}', pulp.LpMinimize)

        p_ch = [pulp.LpVariable(f'pch_{t}', 0, kw_batt) for t in range(H)]
        p_dis = [pulp.LpVariable(f'pdis_{t}', 0, kw_batt) for t in range(H)]
        grid_in = [pulp.LpVariable(f'gin_{t}', 0, max_afname_hard) for t in range(H)]
        grid_out = [pulp.LpVariable(f'gout_{t}', 0, max_injectie_hard) for t in range(H)]
        soc = [pulp.LpVariable(f'soc_{t}', soc_min, soc_max) for t in range(H + 1)]
        over_afn_zacht = [pulp.LpVariable(f'oaz_{t}', 0) for t in range(H)]
        over_inj_zacht = [pulp.LpVariable(f'oiz_{t}', 0) for t in range(H)]

        if pv_curtailment_allowed:
            pv_curt = [pulp.LpVariable(f'pvc_{t}', 0, max(pv_kw[t], 0)) for t in range(H)]
        else:
            pv_curt = [pulp.LpVariable(f'pvc_{t}', 0, 0) for t in range(H)]

        # Nominatie als LP-variabele met fysieke richting-constraint
        nom_afn = []
        nom_inj = []
        for t in range(H):
            if forecast_afn[t] > 0:
                nom_afn.append(pulp.LpVariable(f'nafn_{t}', 0, max_afname_hard))
                nom_inj.append(pulp.LpVariable(f'ninj_{t}', 0, 0))
            else:
                nom_afn.append(pulp.LpVariable(f'nafn_{t}', 0, 0))
                nom_inj.append(pulp.LpVariable(f'ninj_{t}', 0, max_injectie_hard))

        spec_dev_pos = [pulp.LpVariable(f'sd_p_{t}', 0) for t in range(H)]
        spec_dev_neg = [pulp.LpVariable(f'sd_n_{t}', 0) for t in range(H)]

        prob += soc[0] == soc_start_kwh

        for t in range(H):
            prob += grid_in[t] - grid_out[t] + p_dis[t] - p_ch[t] + (pv_kw[t] - pv_curt[t]) == consumption_kw[t]
            prob += soc[t + 1] == soc[t] + eta * p_ch[t] * dt_h - (1.0 / eta) * p_dis[t] * dt_h
            prob += over_afn_zacht[t] >= grid_in[t] - max_afname_zacht
            prob += over_inj_zacht[t] >= grid_out[t] - max_injectie_zacht
            prob += p_ch[t] + p_dis[t] <= kw_batt
            prob += (nom_afn[t] - nom_inj[t]) - (forecast_afn[t] - forecast_inj[t]) == spec_dev_pos[t] - spec_dev_neg[t]

        # Speculation budget volgens multiplier
        if spec_budget_multiplier >= 0 and daily_paper_risk_mwh >= 0:
            effective_budget = daily_paper_risk_mwh * spec_budget_multiplier
            spec_total = pulp.lpSum(spec_dev_pos[t] + spec_dev_neg[t] for t in range(H)) * dt_h / 1000.0
            prob += spec_total <= effective_budget
        # multiplier < 0 → constraint wordt niet opgelegd (volledig vrij)

        # Objective
        obj_terms = []
        eps_penalty = 1.0 / 1000.0  # 1 €/MWh anti-fake-volume (al aanwezig in v1.5 BSP)
        for t in range(H):
            prijs_dam_afn = (spot_eur_mwh[t] + markup_per_mwh) / 1000.0
            prijs_dam_inj = (spot_eur_mwh[t] - markdown_per_mwh) / 1000.0
            prijs_imb = imb_eur_mwh[t] / 1000.0
            belastingen_per_kwh = (gsc + wkk + vergroening) / 1000.0

            obj_terms.append(prijs_dam_afn * nom_afn[t] * dt_h)
            obj_terms.append(-prijs_dam_inj * nom_inj[t] * dt_h)
            obj_terms.append(prijs_imb * (grid_in[t] - nom_afn[t]) * dt_h)
            obj_terms.append(-prijs_imb * (grid_out[t] - nom_inj[t]) * dt_h)
            obj_terms.append(belastingen_per_kwh * grid_in[t] * dt_h)
            obj_terms.append(cyclus_kost_eur_per_kwh * p_dis[t] * dt_h)
            obj_terms.append(pen_afname_zacht * over_afn_zacht[t])
            obj_terms.append(pen_injectie_zacht * over_inj_zacht[t])
            obj_terms.append(eps_penalty * (grid_in[t] + grid_out[t]) * dt_h)

        prob += pulp.lpSum(obj_terms)

        solver = pulp.PULP_CBC_CMD(msg=0, timeLimit=15)
        status = prob.solve(solver)
        status_str = pulp.LpStatus[status]

        # Extract values (zelfs als niet-optimal, voor inspectie)
        nom_afn_vals = [pulp.value(v) or 0.0 for v in nom_afn]
        nom_inj_vals = [pulp.value(v) or 0.0 for v in nom_inj]
        pv_curt_vals = [pulp.value(v) or 0.0 for v in pv_curt]
        p_ch_vals = [pulp.value(v) or 0.0 for v in p_ch]
        p_dis_vals = [pulp.value(v) or 0.0 for v in p_dis]
        grid_in_vals = [pulp.value(v) or 0.0 for v in grid_in]
        grid_out_vals = [pulp.value(v) or 0.0 for v in grid_out]
        soc_vals = [pulp.value(v) or 0.0 for v in soc]
        oaz_vals = [pulp.value(v) or 0.0 for v in over_afn_zacht]
        oiz_vals = [pulp.value(v) or 0.0 for v in over_inj_zacht]

        return status_str, {
            'p_ch': p_ch_vals, 'p_dis': p_dis_vals,
            'grid_in': grid_in_vals, 'grid_out': grid_out_vals,
            'soc': soc_vals,
            'nom_afn': nom_afn_vals, 'nom_inj': nom_inj_vals,
            'pv_curt': pv_curt_vals,
            'oaz': oaz_vals, 'oiz': oiz_vals,
        }

    # ── Retry-ladder ──
    # Niveau 0: normale spec_budget (×1)
    status_str, r = _build_and_solve_bsp(1.0, 'n0')
    retry_level = 0

    if status_str != 'Optimal':
        log.warning(f"BSP-LP niveau 0 non-optimal ({status_str}) — retry niveau 1 (spec_budget × 10)")
        status_str, r = _build_and_solve_bsp(10.0, 'n1')
        retry_level = 1
        if status_str != 'Optimal':
            log.warning(f"BSP-LP niveau 1 non-optimal ({status_str}) — retry niveau 2 (spec_budget verwijderd)")
            status_str, r = _build_and_solve_bsp(-1.0, 'n2')
            retry_level = 2
            if status_str != 'Optimal':
                # Niveau 3: dag wordt overgeslagen. GEEN passieve fallback
                # (design beslissing 4.3 — Fluctus zou op zo'n moment ook geen
                # BSP-flex toepassen, dus business-case mag dit niet vermooien).
                log.warning(
                    f"BSP-LP niveau 2 nog steeds non-optimal ({status_str}) — dag wordt overgeslagen "
                    f"(grid_in/out/p_ch/p_dis = 0 voor 96 kwartieren)"
                )
                retry_level = 3
                # Vul lege arrays
                _zeros = [0.0] * H
                r = {
                    'p_ch': list(_zeros), 'p_dis': list(_zeros),
                    'grid_in': list(_zeros), 'grid_out': list(_zeros),
                    'soc': [soc_start_kwh] * (H + 1),  # SoC blijft constant
                    'nom_afn': list(_zeros), 'nom_inj': list(_zeros),
                    'pv_curt': list(_zeros),
                    'oaz': list(_zeros), 'oiz': list(_zeros),
                }

    # Extract uit dict (helper) naar locals
    p_ch_vals = r['p_ch']
    p_dis_vals = r['p_dis']
    grid_in_vals = r['grid_in']
    grid_out_vals = r['grid_out']
    soc_vals = r['soc']
    nom_afn_vals = r['nom_afn']
    nom_inj_vals = r['nom_inj']
    pv_curt_vals = r['pv_curt']
    oaz_vals = r['oaz']
    oiz_vals = r['oiz']

    # v1.6 post-solve clip (laatste defensie tegen CBC bound-violations bij
    # Optimal-status — zou normaal niet meer voorkomen na verwijdering van
    # tegenstrijdige hard-soft bounds, maar als zekerheid):
    _tol = 0.01
    if grid_in_vals and max(grid_in_vals) > max_afname_hard + _tol:
        log.warning(
            f"BSP grid_in bound-violation: max={max(grid_in_vals):.2f} > cap={max_afname_hard:.2f} — clip toegepast"
        )
        grid_in_vals = [min(v, max_afname_hard) for v in grid_in_vals]
    if grid_out_vals and max(grid_out_vals) > max_injectie_hard + _tol:
        log.warning(
            f"BSP grid_out bound-violation: max={max(grid_out_vals):.2f} > cap={max_injectie_hard:.2f} — clip toegepast"
        )
        grid_out_vals = [min(v, max_injectie_hard) for v in grid_out_vals]
    if p_ch_vals and max(p_ch_vals) > kw_batt + _tol:
        log.warning(f"BSP p_ch bound-violation: max={max(p_ch_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_ch_vals = [min(v, kw_batt) for v in p_ch_vals]
    if p_dis_vals and max(p_dis_vals) > kw_batt + _tol:
        log.warning(f"BSP p_dis bound-violation: max={max(p_dis_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_dis_vals = [min(v, kw_batt) for v in p_dis_vals]

    dev_net_vals = [(grid_in_vals[t] - nom_afn_vals[t]) - (grid_out_vals[t] - nom_inj_vals[t]) for t in range(H)]

    nom_revenue = sum(
        (spot_eur_mwh[t] + markup_per_mwh) / 1000.0 * nom_afn_vals[t] * dt_h -
        (spot_eur_mwh[t] - markdown_per_mwh) / 1000.0 * nom_inj_vals[t] * dt_h
        for t in range(H)
    )

    # Label voor diagnostics
    lp_status_label = (
        'Optimal' if retry_level == 0 else
        'retry1_optimal' if retry_level == 1 else
        'retry2_optimal' if retry_level == 2 else
        'verloren'
    )

    return {
        'p_charge': p_ch_vals,
        'p_discharge': p_dis_vals,
        'grid_in': grid_in_vals,
        'grid_out': grid_out_vals,
        'soc': soc_vals,
        'over_afn_zacht': oaz_vals,
        'over_inj_zacht': oiz_vals,
        # v1.6: over_*_hard zijn vervallen, leveren 0-arrays voor backwards-compat
        'over_afn_hard': [0.0] * H,
        'over_inj_hard': [0.0] * H,
        # BSP-specifiek
        'nom_dam_kw': list(nom_net),
        'nom_eff_afn_kw': nom_afn_vals,
        'nom_eff_inj_kw': nom_inj_vals,
        'dev_kw': dev_net_vals,
        'pv_curtailed_kw': pv_curt_vals,
        'nom_revenue_eur': nom_revenue,
        # v1.6 diagnostics
        'lp_status': lp_status_label,
        'retry_level': retry_level,
    }


# =============================================================================
# BSP-LP PER KALENDERMAAND (v1.7 sessie 7 — Strategie A: Groep B in objective)
# =============================================================================

def lp_dispatch_month_bsp(
    consumption_kw: list,         # H kwartieren — H = n_dagen × 96 voor één maand
    pv_kw: list,                  # H kwartieren
    spot_eur_mwh: list,           # H kwartieren
    imb_eur_mwh: list,            # H kwartieren
    soc_start_kwh: float,
    batterij: dict,
    aansluiting: dict,
    contract: dict,
    cyclus_kost_eur_per_kwh: float,
    paper_capture_rate: float,    # bv 0.018 (1.8%) × forecast_modus_multiplier
    pv_curtailment_allowed: bool,
    netbeheer_tarieven: dict,     # v1.7: nodig voor monthly_peak-kost in objective
    consumption_forecast_kw: list = None,
    pv_forecast_kw: list = None,
) -> dict:
    """
    Maand-niveau BSP-LP. Refactor v1.7 van lp_dispatch_day_bsp.
    Voegt monthly_peak-variabele + Groep B/D capaciteit-kost toe aan objective.

    Werking:
      H = n_dagen × 96 (bv 31 × 96 = 2976 voor juli)
      n_dagen = H // 96

    Constraints (per kwartier t in [0, H)):
      grid_in[t] - grid_out[t] + p_dis[t] - p_ch[t] + (pv[t] - pv_curt[t])
          = consumption[t]
      soc[t+1] = soc[t] + eta·p_ch[t]·dt - (1/eta)·p_dis[t]·dt
      monthly_peak >= grid_in[t]   # ← NIEUW v1.7
      over_afn_zacht[t] >= grid_in[t] - max_afname_zacht
      over_inj_zacht[t] >= grid_out[t] - max_injectie_zacht
      p_ch[t] + p_dis[t] <= kw_batt
      (nom_afn[t] - nom_inj[t]) - (forecast_afn[t] - forecast_inj[t])
          = spec_dev_pos[t] - spec_dev_neg[t]
      Per dag d: sum_{t in dag d} (spec_dev_pos[t] + spec_dev_neg[t]) * dt / 1000
          <= paper_capture_rate × volume_dag_d_MWh

    Objective (samenvattend):
      DAM-tak op nominatie + IMB-tak op deviation + belastingen op fysiek volume
      + cyclus_kost × p_dis + zachte penalties + eps_penalty × volume
      + monthly_peak × c_per_maand    # ← NIEUW v1.7
      waar c_per_maand = tar_maandpiek_B/12 + tar_tr_maandpiek_D
                       + tar_jaarpiek_B/12 + tar_tr_beschikb_D/12

    Retry-ladder bij niet-Optimal:
      Niveau 0 (multiplier 1.0)  → Niveau 1 (×10) → Niveau 2 (geen budget)
      → Niveau 3 (v1.8 PRIORITEIT haalbaarheid: HAALBAAR dispatch met gesoftende
         fysieke aansluitings-caps + zware penalty; de last wordt ALTIJD bediend,
         eventuele overschrijding staat in over_afn_hard/over_inj_hard)
      → Niveau 4 (noodval, zou niet mogen: maand verloren, grid_in/out = 0).

    Returns dict:
      - p_charge[H], p_discharge[H], grid_in[H], grid_out[H], soc[H+1]
      - over_afn_zacht[H], over_inj_zacht[H]
      - over_afn_hard[H], over_inj_hard[H] (> 0 enkel in feasibility-modus niveau 3)
      - nom_dam_kw[H] (= consumption - pv, op forecast),
        nom_eff_afn_kw[H], nom_eff_inj_kw[H], dev_kw[H]
      - pv_curtailed_kw[H]
      - nom_revenue_eur (float)
      - lp_status ('Optimal'/'retry1_optimal'/'retry2_optimal'/'feasibility'/'verloren')
      - retry_level (0/1/2/3/4)
      - monthly_peak_kw (float) — gerealiseerde maandpiek-afname uit LP
    """
    H = len(consumption_kw)
    if H % 96 != 0:
        log.error(f"lp_dispatch_month_bsp: H={H} niet deelbaar door 96, valt terug op dag-modulo.")
    n_dagen = max(1, H // 96)
    dt_h = 0.25

    kw_batt = batterij['kw']
    kwh_batt = batterij['kwh']
    dod = batterij['dod_pct'] / 100.0 if batterij['dod_pct'] > 1.5 else batterij['dod_pct']
    rte = batterij['rte_pct'] / 100.0 if batterij['rte_pct'] > 1.5 else batterij['rte_pct']
    eta = math.sqrt(rte)

    soc_min = 0.0
    soc_max = kwh_batt * dod

    # Aansluiting-bounds (v1.6: hard via LpVariable upper bound, zacht via
    # penalty op over_zacht; geen over_hard meer, zie RCA defect 3).
    tar_afname_kw = aansluiting.get('tarief_overschrijding_afname_eur_per_kw_jaar', 62.47)
    tar_injectie_kw = aansluiting.get('tarief_overschrijding_injectie_eur_per_kw_jaar', 1.0)
    pen_afname_zacht = tar_afname_kw / 12.0
    pen_injectie_zacht = tar_injectie_kw / 12.0

    max_afname_zacht = aansluiting.get('max_afname_kw_zacht', 1e9)
    max_afname_hard = aansluiting.get('max_afname_kw_hard', 1e9)
    max_injectie_zacht = aansluiting.get('max_injectie_kw_zacht', 1e9)
    max_injectie_hard = aansluiting.get('max_injectie_kw_hard', 1e9)

    injectie_toegelaten = contract.get('injectie_toegelaten', True)
    if not injectie_toegelaten:
        max_injectie_zacht = 0.0
        max_injectie_hard = 0.0

    # v1.7: Groep B + D capaciteit-tarieven uit netbeheer.tarieven.
    # Per-maand "marginale" kost voor 1 kW extra in monthly_peak[m]:
    #   - Groep B maandpiek: gem(maandpieken) × tar_maandpiek_B  → per maand: × /12
    #   - Groep D transport-maandpiek: sum(maandpieken) × tar_tr_maandpiek_D  → × 1
    #   - Groep B toegangsvermogen: max(maandpieken) × tar_jaarpiek_B
    #       Benadering uniform-spreid (alsof elke maand met kans 1/12 de jaarpiek
    #       wordt): per maand × /12. Dit is een onderschatting voor de echte
    #       jaarpiek-maand maar geeft LP een consistente prikkel om ALLE
    #       maandpieken te shaven (= robuust). Single-pass jaar-LP zou exacter
    #       zijn maar te zwaar voor CBC; iterative shadow-price komt in v1.8+.
    #   - Groep D beschikbaar vermogen: toegangsvermogen × tar_tr_beschikb_D
    #       Idem benadering uniform-spreid: per maand × /12.
    _tar_maandpiek_B    = (netbeheer_tarieven or {}).get('maandpiek_eur_kw_jaar', 0.0)
    _tar_tr_maandpiek_D = (netbeheer_tarieven or {}).get('transport_maandpiek_eur_kw_mnd', 0.0)
    # v1.8.3: enkel de op GEREALISEERDE piek gefactureerde capaciteitstermen
    # (maandpiek B + transport-maandpiek D) sturen de piekshaving-prikkel.
    # Toegangsvermogen (B) en beschikbaar vermogen (D) zijn nu SUNK (op contract,
    # zie bereken_jaarfactuur v1.8.3) → géén marginale piekkost meer, dus NIET
    # meer in c_per_maand_kw. Zo shaaft de LP de piek enkel waar dat écht bespaart
    # en gebruikt ze de reeds-betaalde kop-ruimte (tot contract) vrij voor arbitrage.
    c_per_maand_kw = (
        _tar_maandpiek_B / 12.0
        + _tar_tr_maandpiek_D
    )

    jaarverbruik_voor_pricing = contract.get('jaarverbruik_mwh', 0.0)
    if not jaarverbruik_voor_pricing:
        jaarverbruik_voor_pricing = sum(consumption_kw) * 0.25 * 365 / (n_dagen if n_dagen else 1) / 1000.0
    pricing = resolve_contract_pricing(contract, jaarverbruik_voor_pricing)

    markup_per_mwh = pricing['markup_dam']
    markdown_per_mwh = pricing['markdown_dam']
    vergroening = pricing['vergroening']
    gsc = pricing['gsc']
    wkk = pricing['wkk']

    # Nominatie = forecast-met-ruis (referentie voor speculation budget)
    cons_for_nom = consumption_forecast_kw if consumption_forecast_kw is not None else consumption_kw
    pv_for_nom = pv_forecast_kw if pv_forecast_kw is not None else pv_kw
    forecast_afn = [max(cons_for_nom[t] - pv_for_nom[t], 0) for t in range(H)]
    forecast_inj = [max(pv_for_nom[t] - cons_for_nom[t], 0) for t in range(H)]
    nom_net = [cons_for_nom[t] - pv_for_nom[t] for t in range(H)]

    # Per-dag papier-budget (in MWh, gebaseerd op werkelijk dagvolume).
    # Anti-regressie t.o.v. v1.6: ZELFDE per-dag budget-mechanisme behouden.
    daily_paper_budget_mwh = []
    for d in range(n_dagen):
        i0 = d * 96
        i1 = min(i0 + 96, H)
        vol_mwh = sum(consumption_kw[i0:i1]) * 0.25 / 1000.0
        daily_paper_budget_mwh.append(paper_capture_rate * vol_mwh)

    def _build_and_solve_month(spec_budget_multiplier: float, label: str,
                               feasibility_only: bool = False):
        """
        v1.7 retry-helper: bouwt en solveert het maand-niveau BSP-LP.
        multiplier=1.0  → normale spec_budget (niveau 0)
        multiplier=10.0 → relaxed spec_budget (niveau 1)
        multiplier=-1   → spec_budget volledig verwijderd (niveau 2)

        v1.8 NIEUW — feasibility_only (niveau 3):
          Prioriteit Johan: EERST voldoen aan bestaand verbruik + laden, DAN
          optimaliseren (modus 2), DAN onbalans (modus 3). Als het BSP-LP zelfs
          zonder spec-budget infeasible blijft (bv. krappe aansluiting + laadplein
          waar de batterij de laadpiek energetisch niet kan dragen), lossen we
          NIET meer op met een verloren maand (grid_in/out=0 → €0, onzin). We
          softenen de FYSIEKE aansluitingslimieten (grid_in/out) met zware
          penalty, zodat het LP ALTIJD een haalbaar dispatch vindt dat de last
          bedient. Overschrijding wordt geregistreerd als over_afn_hard/
          over_inj_hard en gaat als waarschuwing naar de gebruiker.
        """
        prob = pulp.LpProblem(f'battery_dispatch_month_bsp_{label}', pulp.LpMinimize)

        # v1.8: in feasibility-modus zijn de fysieke caps zacht (big-M bound +
        # penalty), zodat de last altijd bediend kan worden.
        _BIG = 1e9
        _PEN_HARD = 1.0e6      # €/kW zware penalty op overschrijding fysieke cap
        # v1.8.4: normale modus → grid_in ≤ min(contract, natuurlijke lastpiek),
        # zodat de sturing de maandpiek niet opblaast om waarde te oogsten.
        # Feasibility-modus → big-M (last moet bediend, overschrijding gepenaliseerd).
        _gin_ub = _BIG if feasibility_only else _gin_cap_normaal(consumption_kw, pv_kw, max_afname_hard)
        _gout_ub = _BIG if feasibility_only else max_injectie_hard

        p_ch = [pulp.LpVariable(f'pch_{t}', 0, kw_batt) for t in range(H)]
        p_dis = [pulp.LpVariable(f'pdis_{t}', 0, kw_batt) for t in range(H)]
        grid_in = [pulp.LpVariable(f'gin_{t}', 0, _gin_ub) for t in range(H)]
        grid_out = [pulp.LpVariable(f'gout_{t}', 0, _gout_ub) for t in range(H)]
        soc = [pulp.LpVariable(f'soc_{t}', soc_min, soc_max) for t in range(H + 1)]
        over_afn_zacht = [pulp.LpVariable(f'oaz_{t}', 0) for t in range(H)]
        over_inj_zacht = [pulp.LpVariable(f'oiz_{t}', 0) for t in range(H)]
        # v1.8: overschrijding van de FYSIEKE aansluiting (enkel actief/gepenaliseerd
        # in feasibility-modus; anders houdt de harde bound ze op 0).
        over_afn_hard = [pulp.LpVariable(f'oah_{t}', 0) for t in range(H)]
        over_inj_hard = [pulp.LpVariable(f'oih_{t}', 0) for t in range(H)]

        # v1.7 NIEUW: monthly_peak — één unieke variabele over de hele maand,
        # bound door grid_in[t] voor alle t. LP minimiseert (cost term in objective),
        # dus zal automatisch = max(grid_in[t]) worden in optimum.
        monthly_peak = pulp.LpVariable('monthly_peak', 0, _gin_ub)

        if pv_curtailment_allowed:
            pv_curt = [pulp.LpVariable(f'pvc_{t}', 0, max(pv_kw[t], 0)) for t in range(H)]
        else:
            pv_curt = [pulp.LpVariable(f'pvc_{t}', 0, 0) for t in range(H)]

        # Nominatie met fysieke richting-constraint per kwartier.
        # v1.8.7 (bug-fix): de DAM-nominatie is een papierpositie begrensd door de
        # FYSIEKE aansluiting — NOOIT de big-M van de feasibility-modus. Anders kon
        # de BSP-papierhandel in feasibility-modus (spec_budget verwijderd + nom-
        # bound 1e9) onbegrensd 'winst' genereren → absurde factuur (miljarden).
        _nom_afn_ub = max_afname_hard
        _nom_inj_ub = max_injectie_hard
        nom_afn = []
        nom_inj = []
        for t in range(H):
            if forecast_afn[t] > 0:
                nom_afn.append(pulp.LpVariable(f'nafn_{t}', 0, _nom_afn_ub))
                nom_inj.append(pulp.LpVariable(f'ninj_{t}', 0, 0))
            else:
                nom_afn.append(pulp.LpVariable(f'nafn_{t}', 0, 0))
                nom_inj.append(pulp.LpVariable(f'ninj_{t}', 0, _nom_inj_ub))

        spec_dev_pos = [pulp.LpVariable(f'sd_p_{t}', 0) for t in range(H)]
        spec_dev_neg = [pulp.LpVariable(f'sd_n_{t}', 0) for t in range(H)]

        # SoC-startwaarde
        prob += soc[0] == soc_start_kwh

        # Per-kwartier constraints
        for t in range(H):
            prob += grid_in[t] - grid_out[t] + p_dis[t] - p_ch[t] + (pv_kw[t] - pv_curt[t]) == consumption_kw[t]
            prob += soc[t + 1] == soc[t] + eta * p_ch[t] * dt_h - (1.0 / eta) * p_dis[t] * dt_h
            prob += monthly_peak >= grid_in[t]  # v1.7 monthly_peak constraint
            prob += over_afn_zacht[t] >= grid_in[t] - max_afname_zacht
            prob += over_inj_zacht[t] >= grid_out[t] - max_injectie_zacht
            # v1.8: registreer fysieke-cap-overschrijding (enkel > 0 in feasibility-modus)
            prob += over_afn_hard[t] >= grid_in[t] - max_afname_hard
            prob += over_inj_hard[t] >= grid_out[t] - max_injectie_hard
            prob += p_ch[t] + p_dis[t] <= kw_batt
            prob += (nom_afn[t] - nom_inj[t]) - (forecast_afn[t] - forecast_inj[t]) == spec_dev_pos[t] - spec_dev_neg[t]

        # Per-dag spec_dev budget constraint
        if spec_budget_multiplier >= 0:
            for d in range(n_dagen):
                i0 = d * 96
                i1 = min(i0 + 96, H)
                if daily_paper_budget_mwh[d] >= 0:
                    effective_budget = daily_paper_budget_mwh[d] * spec_budget_multiplier
                    daily_spec = pulp.lpSum(spec_dev_pos[t] + spec_dev_neg[t] for t in range(i0, i1)) * dt_h / 1000.0
                    prob += daily_spec <= effective_budget
        # multiplier < 0 → geen spec-budget constraint (volledig vrij)

        # Objective
        obj_terms = []
        eps_penalty = 1.0 / 1000.0  # 1 €/MWh anti-fake-volume

        for t in range(H):
            prijs_dam_afn = (spot_eur_mwh[t] + markup_per_mwh) / 1000.0
            prijs_dam_inj = (spot_eur_mwh[t] - markdown_per_mwh) / 1000.0
            prijs_imb = imb_eur_mwh[t] / 1000.0
            belastingen_per_kwh = (gsc + wkk + vergroening) / 1000.0

            obj_terms.append(prijs_dam_afn * nom_afn[t] * dt_h)
            obj_terms.append(-prijs_dam_inj * nom_inj[t] * dt_h)
            obj_terms.append(prijs_imb * (grid_in[t] - nom_afn[t]) * dt_h)
            obj_terms.append(-prijs_imb * (grid_out[t] - nom_inj[t]) * dt_h)
            obj_terms.append(belastingen_per_kwh * grid_in[t] * dt_h)
            obj_terms.append(cyclus_kost_eur_per_kwh * p_dis[t] * dt_h)
            obj_terms.append(pen_afname_zacht * over_afn_zacht[t])
            obj_terms.append(pen_injectie_zacht * over_inj_zacht[t])
            obj_terms.append(eps_penalty * (grid_in[t] + grid_out[t]) * dt_h)
            # v1.8: in feasibility-modus zware penalty op fysieke-cap-overschrijding,
            # zodat het LP eerst batterij/shift inzet en pas als laatste redmiddel
            # de aansluiting overschrijdt (haalbaarheid gegarandeerd).
            if feasibility_only:
                obj_terms.append(_PEN_HARD * (over_afn_hard[t] + over_inj_hard[t]))

        # v1.8.3: piekshaving-prikkel op de GEREALISEERDE maandpiek, met
        # c_per_maand_kw = enkel de realized-gefactureerde capaciteitstermen
        # (maandpiek B + transport-maandpiek D). Toegangsvermogen/beschikbaar zijn
        # sunk (op contract) en zitten niet meer in c_per_maand_kw → de LP gebruikt
        # de kop-ruimte tot contract vrij voor arbitrage, maar shaaft de piek waar
        # dat de maandpiek-kost verlaagt.
        obj_terms.append(c_per_maand_kw * monthly_peak)

        prob += pulp.lpSum(obj_terms)

        # Maand-LP is groter dan dag-LP. CBC timeLimit verhogen tot 60s per maand
        # (acceptatie-criterium sessie 7: ≤60s per scenario ⇒ ~5s per maand zou
        # genoeg moeten zijn, 60s is ruime veiligheidsmarge per maand).
        solver = pulp.PULP_CBC_CMD(msg=0, timeLimit=60)
        status = prob.solve(solver)
        status_str = pulp.LpStatus[status]

        # Extract values
        nom_afn_vals = [pulp.value(v) or 0.0 for v in nom_afn]
        nom_inj_vals = [pulp.value(v) or 0.0 for v in nom_inj]
        pv_curt_vals = [pulp.value(v) or 0.0 for v in pv_curt]
        p_ch_vals = [pulp.value(v) or 0.0 for v in p_ch]
        p_dis_vals = [pulp.value(v) or 0.0 for v in p_dis]
        grid_in_vals = [pulp.value(v) or 0.0 for v in grid_in]
        grid_out_vals = [pulp.value(v) or 0.0 for v in grid_out]
        soc_vals = [pulp.value(v) or 0.0 for v in soc]
        oaz_vals = [pulp.value(v) or 0.0 for v in over_afn_zacht]
        oiz_vals = [pulp.value(v) or 0.0 for v in over_inj_zacht]
        oah_vals = [pulp.value(v) or 0.0 for v in over_afn_hard]
        oih_vals = [pulp.value(v) or 0.0 for v in over_inj_hard]
        mp_val = pulp.value(monthly_peak) or 0.0

        return status_str, {
            'p_ch': p_ch_vals, 'p_dis': p_dis_vals,
            'grid_in': grid_in_vals, 'grid_out': grid_out_vals,
            'soc': soc_vals,
            'nom_afn': nom_afn_vals, 'nom_inj': nom_inj_vals,
            'pv_curt': pv_curt_vals,
            'oaz': oaz_vals, 'oiz': oiz_vals,
            'oah': oah_vals, 'oih': oih_vals,
            'monthly_peak': mp_val,
        }

    # ── Retry-ladder per maand ──
    status_str, r = _build_and_solve_month(1.0, 'n0')
    retry_level = 0

    if status_str != 'Optimal':
        log.warning(f"Maand-BSP-LP niveau 0 non-optimal ({status_str}) — retry niveau 1 (spec_budget × 10)")
        status_str, r = _build_and_solve_month(10.0, 'n1')
        retry_level = 1
        if status_str != 'Optimal':
            log.warning(f"Maand-BSP-LP niveau 1 non-optimal ({status_str}) — retry niveau 2 (spec_budget verwijderd)")
            status_str, r = _build_and_solve_month(-1.0, 'n2')
            retry_level = 2
            if status_str != 'Optimal':
                # v1.8 — PRIORITEIT (Johan): eerst voldoen aan bestaand verbruik +
                # laden. NIET meer de maand op 0 zetten (dat gaf €0 = onzin bij
                # krappe aansluiting + laadplein). We solven een HAALBAAR dispatch
                # met gesoftende fysieke caps: het LP bedient altijd de last en
                # zet batterij/shift maximaal in; enkel als laatste redmiddel
                # overschrijdt het de aansluiting (geregistreerd als over_*_hard).
                log.warning(
                    f"Maand-BSP-LP niveau 2 nog steeds non-optimal ({status_str}) — "
                    f"terugval op HAALBAAR dispatch (feasibility-modus, fysieke caps zacht)"
                )
                # v1.8.7: feasibility-solve houdt een (ruim) spec_budget aan i.p.v.
                # het volledig te verwijderen — zo blijft de BSP-papierhandel
                # begrensd, ook wanneer de fysieke caps gesoftend zijn.
                status_str, r = _build_and_solve_month(10.0, 'n3', feasibility_only=True)
                retry_level = 3
                if status_str != 'Optimal':
                    # Zou niet mogen gebeuren (big-M maakt het altijd haalbaar).
                    # Absolute noodval: constante SoC, geen dispatch.
                    log.error(
                        f"Maand-BSP-LP feasibility-modus ONVERWACHT non-optimal ({status_str}) — "
                        f"noodval: maand overgeslagen ({H} kwartieren op 0)"
                    )
                    retry_level = 4
                    _zeros = [0.0] * H
                    r = {
                        'p_ch': list(_zeros), 'p_dis': list(_zeros),
                        'grid_in': list(_zeros), 'grid_out': list(_zeros),
                        'soc': [soc_start_kwh] * (H + 1),
                        'nom_afn': list(_zeros), 'nom_inj': list(_zeros),
                        'pv_curt': list(_zeros),
                        'oaz': list(_zeros), 'oiz': list(_zeros),
                        'oah': list(_zeros), 'oih': list(_zeros),
                        'monthly_peak': 0.0,
                    }

    # Extract uit dict
    p_ch_vals = r['p_ch']
    p_dis_vals = r['p_dis']
    grid_in_vals = r['grid_in']
    grid_out_vals = r['grid_out']
    soc_vals = r['soc']
    nom_afn_vals = r['nom_afn']
    nom_inj_vals = r['nom_inj']
    pv_curt_vals = r['pv_curt']
    oaz_vals = r['oaz']
    oiz_vals = r['oiz']
    oah_vals = r.get('oah', [0.0] * H)
    oih_vals = r.get('oih', [0.0] * H)
    monthly_peak_kw = r['monthly_peak']

    # v1.6 post-solve clip (defensieve clip, identiek aan dag-versie).
    # v1.8: NIET clippen in feasibility-modus (retry_level == 3) — daar
    # OVERSCHRIJDT grid_in bewust de fysieke cap om de last te bedienen;
    # clippen zou de energiebalans breken. De overschrijding zit in oah/oih.
    _tol = 0.01
    if retry_level < 3 and grid_in_vals and max(grid_in_vals) > max_afname_hard + _tol:
        log.warning(
            f"Maand-BSP grid_in bound-violation: max={max(grid_in_vals):.2f} > cap={max_afname_hard:.2f} — clip toegepast"
        )
        grid_in_vals = [min(v, max_afname_hard) for v in grid_in_vals]
    if retry_level < 3 and grid_out_vals and max(grid_out_vals) > max_injectie_hard + _tol:
        log.warning(
            f"Maand-BSP grid_out bound-violation: max={max(grid_out_vals):.2f} > cap={max_injectie_hard:.2f} — clip toegepast"
        )
        grid_out_vals = [min(v, max_injectie_hard) for v in grid_out_vals]
    if p_ch_vals and max(p_ch_vals) > kw_batt + _tol:
        log.warning(f"Maand-BSP p_ch bound-violation: max={max(p_ch_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_ch_vals = [min(v, kw_batt) for v in p_ch_vals]
    if p_dis_vals and max(p_dis_vals) > kw_batt + _tol:
        log.warning(f"Maand-BSP p_dis bound-violation: max={max(p_dis_vals):.2f} > cap={kw_batt:.2f} — clip toegepast")
        p_dis_vals = [min(v, kw_batt) for v in p_dis_vals]

    dev_net_vals = [(grid_in_vals[t] - nom_afn_vals[t]) - (grid_out_vals[t] - nom_inj_vals[t]) for t in range(H)]

    nom_revenue = sum(
        (spot_eur_mwh[t] + markup_per_mwh) / 1000.0 * nom_afn_vals[t] * dt_h -
        (spot_eur_mwh[t] - markdown_per_mwh) / 1000.0 * nom_inj_vals[t] * dt_h
        for t in range(H)
    )

    lp_status_label = (
        'Optimal' if retry_level == 0 else
        'retry1_optimal' if retry_level == 1 else
        'retry2_optimal' if retry_level == 2 else
        'feasibility' if retry_level == 3 else
        'verloren'
    )

    return {
        'p_charge': p_ch_vals,
        'p_discharge': p_dis_vals,
        'grid_in': grid_in_vals,
        'grid_out': grid_out_vals,
        'soc': soc_vals,
        'over_afn_zacht': oaz_vals,
        'over_inj_zacht': oiz_vals,
        # v1.8: over_*_hard = fysieke-cap-overschrijding (> 0 enkel in feasibility-modus)
        'over_afn_hard': oah_vals,
        'over_inj_hard': oih_vals,
        # BSP-specifiek
        'nom_dam_kw': list(nom_net),
        'nom_eff_afn_kw': nom_afn_vals,
        'nom_eff_inj_kw': nom_inj_vals,
        'dev_kw': dev_net_vals,
        'pv_curtailed_kw': pv_curt_vals,
        'nom_revenue_eur': nom_revenue,
        # Diagnostics
        'lp_status': lp_status_label,
        'retry_level': retry_level,
        # v1.7 NIEUW
        'monthly_peak_kw': monthly_peak_kw,
        'n_dagen': n_dagen,
    }


# =============================================================================
# JAARFACTUUR (volgens Facturatielogica.xlsx)
# =============================================================================

def bereken_jaarfactuur(
    grid_in_kw: list,              # N kwartieren — fysiek werkelijk afgenomen
    grid_out_kw: list,             # N kwartieren — fysiek werkelijk geïnjecteerd
    spot_eur_mwh: list,            # N kwartieren (werkelijk, niet forecast)
    imb_eur_mwh: list,             # N kwartieren (werkelijk, alleen relevant voor passthrough)
    timestamps: list,              # list[datetime], N kwartieren
    contract: dict,
    netbeheer: dict,
    n_maanden: int = 12,
    nom_afn_kw_all: list = None,   # v1.4 BSP: genomineerde afname (None=passive default)
    nom_inj_kw_all: list = None,   # v1.4 BSP: genomineerde injectie
    effectief_jaarverbruik_voor_pricing: float = None,  # v1.5 sessie 4
    aansluiting: dict = None,      # v1.6 sessie 6: voor twee-bucket Groep B
) -> dict:
    """
    Bereken jaarfactuur volgens groepen A-E.
    
    BSP-uitbreiding (v1.4):
    - Als nom_afn_kw_all en nom_inj_kw_all zijn gegeven, splits de DAM/IMB-tak:
        DAM-tak rekent op nominatie (nom_afn × DAM, nom_inj × DAM)
        IMB-tak rekent op deviation (grid_in - nom_afn) × IMB
        Belastingen (gsc/wkk/vergr) op fysiek volume (grid_in)
    - Als ze None zijn → passive gedrag (alle volume via DAM zoals voorheen)
    
    Specifieke-periode uitbreiding (v1.5 sessie 4):
    - n_maanden: aantal kalendermaanden in de simulatieperiode (voor prorata
      van vaste-kost componenten). Default 12 = volledig jaar = identiek
      aan v1.4 gedrag.
    - effectief_jaarverbruik_voor_pricing: bij type="specifiek" wordt dit
      meegegeven als het GEPROJECTEERDE jaarverbruik (=periode_volume /
      profielfractie). Het wordt gebruikt voor:
        (a) Enwyse-staffel-tier-keuze in resolve_contract_pricing
        (b) Accijnzen-staffel-keuze
      Het BEDRAG van energie/MWh-componenten en accijnzen blijft op het
      werkelijke periode-volume gebaseerd; alleen de TARIEF-KEUZE schakelt
      naar het projecteerde jaarniveau.
      Bij None (default): tier en accijnzen op periode-volume = v1.4 gedrag.

    Aansluiting-uitbreiding (v1.6 sessie 6):
    - aansluiting: optioneel dict met max_afname_kw_hard. Wanneer meegegeven
      wordt Groep B maandpiek opgesplitst in twee buckets:
        gem_binnen = gem(min(maandpiek, cap)) × tarief_maandpiek
        gem_over   = gem(max(0, maandpiek - cap)) × tarief_overschrijding
      Dit modelleert het Belgische tarief correct bij scenarios waar de
      maandpiek de aansluiting overschrijdt (kan in v1.6 nog gebeuren wanneer
      profielpiek > contractueel aansluitvermogen en geen BESS).
      Bij None (default): v1.5-gedrag behouden (overschrijding ten opzichte
      van jaar-toegangsvermogen).
    
    v1.6 OUTPUT-WIJZIGING:
    - jaarpiek_afname_kw is nu = max(maandpieken_afname) (was: winter-piek).
    - jaarpiek_injectie_kw nieuw = max(maandpieken_injectie).
    - toegangsvermogen_kw blijft alias voor jaarpiek_afname_kw (backwards-compat).
    - winterpiek_afname_kw nieuw veld bewaart de v1.5-semantiek (nov-mrt piek
      gebruikt voor Elia transport-jaarpiek). De Elia-transportkost wordt
      INTERN nog steeds berekend op de winter-piek, dus geen factuur-impact.
    
    Returns: dict met groepen en totaal.
    """
    N = len(grid_in_kw)
    dt_h = 0.25
    
    # BSP-modus actief? (= nominatie-arrays gegeven)
    bsp_modus = (nom_afn_kw_all is not None) and (nom_inj_kw_all is not None)
    
    # Default: nominatie = werkelijk volume (passive equivalence)
    if not bsp_modus:
        nom_afn_kw_all = list(grid_in_kw)
        nom_inj_kw_all = list(grid_out_kw)

    tarieven = netbeheer['tarieven']
    spanning = netbeheer['spanning']  # 'MS' of 'LS'

    # v1.5 sessie 4: prorata-factor voor vaste-kost componenten.
    # Bij n_maanden=12 (default): factor=1.0, IDENTIEK aan v1.4 gedrag.
    # Bij type="specifiek" wordt caller geacht n_maanden te zetten op het
    # aantal kalendermaanden in [van, tot) — bv. 1 voor jan-only.
    prorata_factor = n_maanden / 12.0

    # Fysiek (werkelijk) volume in kWh
    afname_kwh_per_kwartier = [grid_in_kw[i] * dt_h for i in range(N)]
    injectie_kwh_per_kwartier = [grid_out_kw[i] * dt_h for i in range(N)]
    totaal_afname_mwh = sum(afname_kwh_per_kwartier) / 1000.0
    totaal_injectie_mwh = sum(injectie_kwh_per_kwartier) / 1000.0
    
    # Nominatie-volume in kWh (= grid in passive, ander in BSP)
    nom_afn_kwh_per_kwartier = [nom_afn_kw_all[i] * dt_h for i in range(N)]
    nom_inj_kwh_per_kwartier = [nom_inj_kw_all[i] * dt_h for i in range(N)]
    totaal_nom_afn_mwh = sum(nom_afn_kwh_per_kwartier) / 1000.0
    totaal_nom_inj_mwh = sum(nom_inj_kwh_per_kwartier) / 1000.0
    
    # Deviation per kwartier (positief = extra afname / minder injectie t.o.v. nominatie)
    # afname_dev_kw[t] = grid_in[t] - nom_afn[t]  (kan + of -)
    # injectie_dev_kw[t] = grid_out[t] - nom_inj[t] (kan + of -)
    afname_dev_kw = [grid_in_kw[i] - nom_afn_kw_all[i] for i in range(N)]
    injectie_dev_kw = [grid_out_kw[i] - nom_inj_kw_all[i] for i in range(N)]

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

    # v1.6: jaarpieken expliciet afname EN injectie.
    # jaarpiek_afname_kw = max(maandpieken_afname) — was in v1.5 een aparte
    # winter-piek (nov-mrt). Voor backwards-compat met de Elia-transport-
    # jaarpiek berekening bewaren we de winter-piek apart als winterpiek_afname_kw.
    jaarpiek_afname = max(maandpieken_afname) if maandpieken_afname else 0
    jaarpiek_injectie = max(maandpieken_injectie) if maandpieken_injectie else 0
    # v1.8.2: capaciteits-basis voor facturatie = GECONTRACTEERD toegangsvermogen
    # (komt uit de klantfactuur; = aansluiting.max_afname_kw). Die netkost is
    # 'sunk': ze staat vast, ongeacht hoe de sturing de reeds-betaalde kop-ruimte
    # benut. Enkel de gerealiseerde maandpiek BOVEN contract is overschrijding.
    # Zonder contract-getal (bv. echte EAN-profielen die het niet meegeven):
    # val terug op de gerealiseerde jaarpiek (= hoogste maandpiek over de periode).
    _contract_kw = 0.0
    if aansluiting:
        _contract_kw = float(aansluiting.get('toegangsvermogen_kw')
                             or aansluiting.get('max_afname_kw_hard') or 0.0)
    if _contract_kw and _contract_kw > 0:
        toegangsvermogen = math.ceil(_contract_kw)   # gecontracteerd (sunk basis)
    else:
        toegangsvermogen = jaarpiek_afname           # fallback: gerealiseerd

    # Winter-piek (nov-mrt) voor Elia transport-jaarpiek-component (v1.5-semantiek).
    winter_kw = []
    for i, ts in enumerate(timestamps):
        if ts.month in [1, 2, 3, 11, 12]:
            winter_kw.append(grid_in_kw[i])
    winterpiek_afname = math.ceil(max(winter_kw)) if winter_kw else 0

    # ---- GROEP A: ENERGIEKOST (commodity) ----
    # v1.4 refactor: DAM-tak rekent op nominatie, IMB-tak op deviation, belastingen op fysiek volume.
    # In passive-modus (geen BSP) zijn nominatie = werkelijk → identiek aan oude logica.
    # v1.5 sessie 4: tier-keuze gebruikt effectief jaarverbruik bij type="specifiek",
    # zodat klant niet in te lage schijf valt als periode-volume <<< jaarvolume.
    # Bij None (default): zelfde gedrag als v1.4 (tier op periode/totaal volume).
    _vol_voor_pricing = (
        effectief_jaarverbruik_voor_pricing
        if effectief_jaarverbruik_voor_pricing is not None and effectief_jaarverbruik_voor_pricing > 0
        else totaal_afname_mwh
    )
    pricing = resolve_contract_pricing(contract, _vol_voor_pricing)
    A = {}
    # A1. Afname energie spot — op NOMINATIE (in passive: = grid_in)
    A['afname_energie_spot'] = sum(
        nom_afn_kwh_per_kwartier[i] * spot_eur_mwh[i] / 1000.0 for i in range(N)
    )
    # A2. Markup afname (DAM-tarief uit schijf) — op NOMINATIE-volume
    A['markup_afname'] = totaal_nom_afn_mwh * pricing['markup_dam']
    # A3-5. Belastingen op FYSIEK volume (vergroening, gsc, wkk)
    # Deze zijn netvergoedingen voor wat fysiek door de meter ging.
    A['vergroening'] = totaal_afname_mwh * pricing['vergroening']
    A['gsc'] = totaal_afname_mwh * pricing['gsc']
    A['wkk'] = totaal_afname_mwh * pricing['wkk']
    # A6. Imbalance afname
    # In passive: dev = 0 → imbalance = 0 (consistent met v1.4-fix)
    # In BSP: dev × IMB per kwartier
    # In forfaitair: vast forfait per MWh op fysiek volume
    if pricing['modus'] == 'forfaitair':
        A['imbalance_afname'] = totaal_afname_mwh * pricing['markup_imb']
    else:
        # Passthrough (passief of BSP): IMB op deviation × IMB-prijs
        # In passive is dev=0 dus = 0 (correct).
        # In BSP rekent dit (grid_in - nom_afn) × IMB per kwartier.
        A['imbalance_afname'] = sum(
            afname_dev_kw[i] * dt_h * imb_eur_mwh[i] / 1000.0 for i in range(N)
        )
    # A7. Vaste kost leverancier (€/maand × aantal maanden)
    # v1.5: was × 12 hardcoded. Nu × n_maanden (default 12 → identiek aan v1.4).
    A['vaste_kost_leverancier'] = pricing['vaste_kost_maand'] * n_maanden
    # A8. Injectie energie spot (negatief = inkomst) — op NOMINATIE
    A['injectie_energie_spot'] = -sum(
        nom_inj_kwh_per_kwartier[i] * spot_eur_mwh[i] / 1000.0 for i in range(N)
    )
    # A9. Markdown injectie (positief, want minder inkomst) — op NOMINATIE-volume
    A['markdown_injectie'] = totaal_nom_inj_mwh * pricing['markdown_dam']
    # A10. Imbalance injectie — zelfde logica als A6
    if pricing['modus'] == 'forfaitair':
        A['imbalance_injectie'] = totaal_injectie_mwh * pricing['markdown_imb']
    else:
        # IMB op deviation × IMB-prijs (negatief = inkomst, want injectie-deviation = extra verkoop)
        A['imbalance_injectie'] = -sum(
            injectie_dev_kw[i] * dt_h * imb_eur_mwh[i] / 1000.0 for i in range(N)
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

    # v1.8.6 — capaciteitskost bij overschrijding (RCA Johan):
    #   Een overschrijding van het toegangsvermogen draagt TWEE kosten, niet één:
    #    (1) MAANDPIEK (capaciteitstarief) op de VOLLEDIGE gerealiseerde maandpiek
    #        (ondergrens 2,5 kW) — NIET gecapt op contract. Piek boven contract
    #        tilt de maandpiek dus mee omhoog.
    #    (2) TOEGANGSVERMOGEN op max(contract, gerealiseerde jaarpiek): ≤ contract
    #        blijft sunk (kop-ruimte gratis); erboven stijgt het naar het
    #        gerealiseerde niveau (de facto: je zou moeten verhogen).
    #    (3) OVERSCHRIJDING = extra penalty op het deel boven contract (Fluvius LS
    #        heeft toegangsvermogen=0 maar overschrijding>0; MS omgekeerd).
    #   Zo kost een overschrijding van X kW ≈ X × (maandpiek + toegangsvermogen)
    #   + X × overschrijding — precies de kost die je met verhogen/batterij vermijdt.
    cap_contract = toegangsvermogen  # = gecontracteerd (of realized-fallback)
    _mp = lambda p: max(p, 2.5) if p > 0 else 0.0
    gem_maandpiek = sum(_mp(p) for p in maandpieken_afname) / 12.0     # VOLLE realized
    maandpieken_over = [max(0, p - cap_contract) for p in maandpieken_afname]
    gem_over = sum(maandpieken_over) / 12.0
    B['maandpiek'] = gem_maandpiek * tar_maandpiek                     # volledige gerealiseerde piek
    B['overschrijding_toegangsvermogen'] = gem_over * tar_overschr     # penalty boven contract
    # Toegangsvermogen op het GECONTRACTEERDE niveau (sunk). Bij overschrijding
    # blijft dit op contract — de klant verhoogt niet, maar betaalt de
    # overschrijdingspenalty (hierboven). Verhoogt de klant wél (verhogen-
    # opstelling), dan is cap_contract al het verhoogde niveau → dan draagt deze
    # term correct de verhogingskost (jouw formule: verhoging × toegangsvermogen).
    B['toegangsvermogen'] = cap_contract * tar_jaarpiek * prorata_factor
    # Proportioneel kWh
    B['proportioneel'] = totaal_afname_mwh * tar_prop
    # Reactief: cosφ=1 in v1 → 0
    B['reactief'] = 0.0
    # Databeheer (€/jaar) — v1.5: × prorata_factor (default 1.0 = identiek aan v1.4)
    B['databeheer'] = tar_databeheer * prorata_factor
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
    # Databeheer_injectie en vaste_vergoeding zijn €/jaar — v1.5: × prorata_factor.
    # (Bij default n_maanden=12: factor=1.0, identiek aan v1.4.)
    C['databeheer_injectie'] = tar_inj_databeheer * prorata_factor
    C['vaste_vergoeding'] = tar_inj_vaste * prorata_factor
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

    # v1.8.6: transport-MAANDPIEK op de VOLLEDIGE gerealiseerde maandpiek (stijgt
    # mee bij overschrijding, net als de distributie-maandpiek).
    D['maandpiek_transport'] = sum(max(p, 0) for p in maandpieken_afname) * tar_tr_maandpiek
    # Elia transport-jaarpiek: gerealiseerde WINTER-piek (nov-mrt), vol.
    D['jaarpiek_transport'] = winterpiek_afname * tar_tr_jaarpiek
    D['systeembeheer'] = totaal_afname_mwh * tar_tr_systeem
    D['reserves'] = totaal_afname_mwh * tar_tr_reserves
    D['marktintegratie'] = totaal_afname_mwh * tar_tr_markt
    D['beschikbaar_vermogen'] = cap_contract * tar_tr_beschikb * prorata_factor
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

    if effectief_jaarverbruik_voor_pricing is not None and effectief_jaarverbruik_voor_pricing > 0:
        # v1.5 sessie 4: bij type="specifiek" wordt de schijf bepaald op het
        # GEPROJECTEERDE jaarverbruik (zodat klant niet in te lage schijf valt),
        # daarna geprorateerd naar werkelijke periode-volume via volume-fractie.
        # Berekening:
        #   1. Volledig jaarbedrag bij effectief_jaarverbruik (cumulatief over schijven)
        #   2. accijns_periode = jaarbedrag × (totaal_afname_mwh / effectief_jaarverbruik)
        # Dit komt mathematisch overeen met "gemiddeld tarief over schijven" × periode-volume.
        jaarbedrag_acc = effectief_jaarverbruik_voor_pricing * accijns_basis
        rest_mwh = effectief_jaarverbruik_voor_pricing
        vorig_grens = 0
        for grens, tarief in accijnzen_staffel:
            schijf_mwh = max(0, min(rest_mwh, grens - vorig_grens))
            if schijf_mwh <= 0:
                break
            jaarbedrag_acc += schijf_mwh * tarief
            rest_mwh -= schijf_mwh
            vorig_grens = grens
            if rest_mwh <= 0:
                break
        accijns_totaal = jaarbedrag_acc * (totaal_afname_mwh / effectief_jaarverbruik_voor_pricing)
    else:
        # v1.4 gedrag: cumulatief op periode-volume direct.
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

    # Energiefonds Vlaanderen (vast €/jaar, BTW-vrij) — v1.5: × prorata_factor.
    E['energiefonds_vlaanderen'] = tarieven.get('energiefonds_eur_jaar', 0.0) * prorata_factor
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
        # v1.6: expliciete splitsing afname/injectie. toegangsvermogen_kw blijft
        # als backwards-compat alias voor jaarpiek_afname_kw.
        'jaarpiek_afname_kw': jaarpiek_afname,        # = max(maandpieken_afname)
        'jaarpiek_injectie_kw': jaarpiek_injectie,    # NIEUW v1.6
        'toegangsvermogen_kw': toegangsvermogen,      # alias = jaarpiek_afname_kw
        # v1.6: winter-piek (nov-mrt) bewaard voor traceability — was in v1.5
        # de betekenis van jaarpiek_afname_kw.
        'winterpiek_afname_kw': winterpiek_afname,
        'maand_mwh_afname': maand_mwh_afname,
        'maand_mwh_injectie': maand_mwh_injectie,
    }


# =============================================================================
# HOOFDSIMULATIE
# =============================================================================


def lp_dispatch_day_stacked(
    consumption_kw: list,         # 96 kwartieren — werkelijk verbruik
    pv_kw: list,                  # 96 kwartieren — PV (0 voor battery-only)
    spot_eur_mwh: list,           # 96 kwartieren — DAM prijs
    imb_eur_mwh: list,            # 96 kwartieren — IMB prijs (perfect foresight)
    soc_start_kwh: float,
    batterij: dict,
    aansluiting: dict,
    contract: dict,
    cyclus_kost_eur_per_kwh: float,
) -> dict:
    """
    Stacked batterij-dispatch: twee-staps LP.

    Stap 1 (DAM-LP): optimaliseer op spot → geeft nominatie grid_in/out
    Stap 2 (IMB-LP): optimaliseer op IMB  → geeft werkelijke grid_in/out

    Factuur:
      - DAM-component: nominatie × (spot + markup)
      - IMB-component: (werkelijk - nominatie) × IMB (geen markup)
      - Cyclus-kost: op ontladen volume

    Dit modelleert de ref "Realistic/Stacked":
      de batterij nomineert op DAM, realiseert op IMB-spreads.
    """
    H = 96
    dt_h = 0.25

    kw_batt = batterij['kw']
    kwh_batt = batterij['kwh']
    dod = batterij['dod_pct'] / 100.0 if batterij['dod_pct'] > 1.5 else batterij['dod_pct']
    rte = batterij['rte_pct'] / 100.0 if batterij['rte_pct'] > 1.5 else batterij['rte_pct']
    eta = math.sqrt(rte)

    soc_min = 0.0
    soc_max = kwh_batt * dod

    max_afname_hard = aansluiting.get('max_afname_kw_hard', 1e9)
    max_injectie_hard = aansluiting.get('max_injectie_kw_hard', 1e9)
    tar_afname = aansluiting.get('tarief_overschrijding_afname_eur_per_kw_jaar', 62.47)
    tar_injectie = aansluiting.get('tarief_overschrijding_injectie_eur_per_kw_jaar', 1.0)
    # v1.6: alleen zachte penalties. Harde bounds zitten in LpVariable upper bound.
    pen_afname_zacht = tar_afname / 12.0
    pen_injectie_zacht = tar_injectie / 12.0
    max_afname_zacht   = aansluiting.get('max_afname_kw_zacht', 1e9)
    max_injectie_zacht = aansluiting.get('max_injectie_kw_zacht', 1e9)

    jaarverb = contract.get('jaarverbruik_mwh', 200.0)
    pricing  = resolve_contract_pricing(contract, jaarverb)
    markup   = pricing['markup_dam']
    markdown = pricing['markdown_dam']

    # v1.8.4: grid_in nooit boven de natuurlijke lastpiek (geen piek-inflatie).
    _gin_cap = _gin_cap_normaal(consumption_kw, pv_kw, max_afname_hard)

    def solve_lp(prijs: list, label: str):
        """Generieke LP: minimaliseer energiekost op gegeven prijs."""
        prob = pulp.LpProblem(f'batt_stacked_{label}', pulp.LpMinimize)
        p_ch  = [pulp.LpVariable(f'pch_{t}',  0, kw_batt)         for t in range(H)]
        p_dis = [pulp.LpVariable(f'pdis_{t}', 0, kw_batt)         for t in range(H)]
        gin   = [pulp.LpVariable(f'gin_{t}',  0, _gin_cap)        for t in range(H)]
        gout  = [pulp.LpVariable(f'gout_{t}', 0, max_injectie_hard) for t in range(H)]
        soc   = [pulp.LpVariable(f'soc_{t}',  soc_min, soc_max)   for t in range(H+1)]
        oaz   = [pulp.LpVariable(f'oaz_{t}',  0) for t in range(H)]
        oiz   = [pulp.LpVariable(f'oiz_{t}',  0) for t in range(H)]

        prob += soc[0] == soc_start_kwh

        for t in range(H):
            net_kw = consumption_kw[t] - pv_kw[t]
            prob += gin[t] - gout[t] + p_dis[t] - p_ch[t] == net_kw
            prob += soc[t+1] == soc[t] + eta * p_ch[t] * dt_h - (1.0/eta) * p_dis[t] * dt_h
            prob += p_ch[t] + p_dis[t] <= kw_batt
            prob += oaz[t] >= gin[t] - max_afname_zacht
            prob += oiz[t] >= gout[t] - max_injectie_zacht

        obj = []
        # v1.6: anti-fake-volume eps op (gin + gout)
        eps_penalty = 1.0 / 1000.0
        for t in range(H):
            prijs_afn = (prijs[t] + markup) / 1000.0
            prijs_inj = (prijs[t] - markdown) / 1000.0
            obj.append(prijs_afn  * gin[t]  * dt_h)
            obj.append(-prijs_inj * gout[t] * dt_h)
            obj.append(cyclus_kost_eur_per_kwh * p_dis[t] * dt_h)
            obj.append(pen_afname_zacht   * oaz[t])
            obj.append(pen_injectie_zacht * oiz[t])
            obj.append(eps_penalty * (gin[t] + gout[t]) * dt_h)

        prob += pulp.lpSum(obj)
        # v1.6: opvangen van status (was weggegooid in v1.5 — RCA defect 2).
        status = prob.solve(pulp.PULP_CBC_CMD(msg=0, timeLimit=10))
        status_str = pulp.LpStatus[status]
        if status_str != 'Optimal':
            log.warning(f"Stacked-LP ({label}) non-optimal: {status_str}")

        gin_vals = [pulp.value(v) or 0.0 for v in gin]
        gout_vals = [pulp.value(v) or 0.0 for v in gout]
        pch_vals = [pulp.value(v) or 0.0 for v in p_ch]
        pdis_vals = [pulp.value(v) or 0.0 for v in p_dis]

        # v1.6 post-solve clip
        _tol = 0.01
        if gin_vals and max(gin_vals) > max_afname_hard + _tol:
            log.warning(f"Stacked-LP ({label}) gin bound-violation: max={max(gin_vals):.2f} > cap={max_afname_hard:.2f} — clip")
            gin_vals = [min(v, max_afname_hard) for v in gin_vals]
        if gout_vals and max(gout_vals) > max_injectie_hard + _tol:
            log.warning(f"Stacked-LP ({label}) gout bound-violation: max={max(gout_vals):.2f} > cap={max_injectie_hard:.2f} — clip")
            gout_vals = [min(v, max_injectie_hard) for v in gout_vals]
        if pch_vals and max(pch_vals) > kw_batt + _tol:
            pch_vals = [min(v, kw_batt) for v in pch_vals]
        if pdis_vals and max(pdis_vals) > kw_batt + _tol:
            pdis_vals = [min(v, kw_batt) for v in pdis_vals]

        return {
            'p_charge':    pch_vals,
            'p_discharge': pdis_vals,
            'grid_in':     gin_vals,
            'grid_out':    gout_vals,
            'soc':         [pulp.value(v) or 0.0 for v in soc],
            'over_afn_zacht':  [pulp.value(v) or 0.0 for v in oaz],
            'over_inj_zacht':  [pulp.value(v) or 0.0 for v in oiz],
            # v1.6: over_*_hard zijn vervallen — leveren 0-arrays voor backwards-compat
            'over_afn_hard':   [0.0] * H,
            'over_inj_hard':   [0.0] * H,
            'lp_status':       status_str,
        }

    # Stap 1: nominatie op DAM
    dam_result = solve_lp(spot_eur_mwh, 'dam')
    nom_grid_in  = dam_result['grid_in']
    nom_grid_out = dam_result['grid_out']
    nom_soc_end  = dam_result['soc'][-1]

    # Stap 2: werkelijke dispatch op IMB
    imb_result = solve_lp(imb_eur_mwh, 'imb')

    # Combineer: werkelijke fysieke dispatch + nominatie voor factuurdecompositie
    result = dict(imb_result)
    result['nom_grid_in']  = nom_grid_in
    result['nom_grid_out'] = nom_grid_out
    return result


def run_simulation(inp: dict) -> dict:
    log.info("=== Fluctus Simulator v1.7 — start ===")

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

    # ---- v1.5 sessie 4: detecteer simulatieperiode-type ----
    # type="specifiek": jaarverbruik_mwh wordt geinterpreteerd als periode-volume.
    #                   Bereken effectief jaarverbruik en n_kalendermaanden voor
    #                   doorgave aan build_consumption_profile en bereken_jaarfactuur.
    # type ontbreekt of != "specifiek": v1.4 gedrag (jaarverbruik_mwh = jaarvolume,
    #                                    n_maanden hardcoded 12, geen tier-override).
    periode_type = (inp['simulatieperiode'].get('type') or 'kalenderjaar').lower()
    is_specifiek = (periode_type == 'specifiek')

    # Default: identiek aan v1.4
    effectief_jaarverbruik_mwh = None
    n_maanden_voor_factuur = 12
    jaarverbruik_mwh_voor_profiel = inp['jaarverbruik_mwh']

    if is_specifiek:
        # Periode-volume in MWh (= factuur-afname)
        periode_volume_mwh = inp['jaarverbruik_mwh']
        # Profielfractie in periode via weekdag-bewuste 2025-mapping
        profielfractie = _profielfractie_in_periode(inp['profiel_kwartier'], sim_timestamps)
        if profielfractie <= 0:
            log.error(
                f"type=specifiek maar profielfractie_in_periode={profielfractie:.6f} ≤ 0. "
                f"Kan effectief jaarverbruik niet berekenen — val terug op v1.4 gedrag."
            )
            is_specifiek = False
        else:
            effectief_jaarverbruik_mwh = periode_volume_mwh / profielfractie
            n_maanden_voor_factuur = _n_kalendermaanden_in_periode(sim_timestamps)
            # Geef effectief jaarverbruik door aan build_consumption_profile zodat
            # de SOM van consumption_kw over de periode = periode_volume_mwh.
            # Wiskundig: effectief × profielfractie = periode_volume → klopt.
            jaarverbruik_mwh_voor_profiel = effectief_jaarverbruik_mwh
            log.info(
                f"type=specifiek: periode_volume={periode_volume_mwh:.2f} MWh, "
                f"profielfractie={profielfractie:.4f}, "
                f"effectief_jaarverbruik={effectief_jaarverbruik_mwh:.2f} MWh, "
                f"n_kalendermaanden={n_maanden_voor_factuur}"
            )

    # ---- Profielen opbouwen ----
    log.info("Profielen opbouwen…")
    consumption_kw = build_consumption_profile(
        inp['profiel_kwartier'],
        jaarverbruik_mwh_voor_profiel,
        inp.get('aanvullingen', {}),
        sim_timestamps,
    )
    log.info(f"Consumptie: max={max(consumption_kw):.1f} kW, gem={sum(consumption_kw)/N:.1f} kW, totaal={sum(consumption_kw)*0.25/1000:.1f} MWh")

    # ── LAADPLEINEN (v1.8): bestaand EV-verbruik uit het basisprofiel schalen ──
    # De energie van BESTAANDE laadpleinen zit al in het factuur-jaarverbruik.
    # We schalen die proportioneel weg zodat de periode-som klopt; alle laadpleinen
    # (bestaand + nieuw) worden daarna als flexibele EV-last toegevoegd (na marktdata).
    _lp_prep = _laadplein_prep(inp, sim_timestamps)
    if _lp_prep['bestaand_periode_mwh'] > 0:
        _base_mwh = sum(consumption_kw) * 0.25 / 1000.0
        if _base_mwh > _lp_prep['bestaand_periode_mwh']:
            _factor = 1.0 - _lp_prep['bestaand_periode_mwh'] / _base_mwh
            consumption_kw = [c * _factor for c in consumption_kw]
            log.info(f"Laadpleinen: {_lp_prep['bestaand_periode_mwh']:.2f} MWh bestaand EV-verbruik uit basisprofiel geschaald (factor {_factor:.3f})")
        else:
            log.warning(f"Bestaand EV-verbruik ({_lp_prep['bestaand_periode_mwh']:.2f} MWh) >= basisverbruik ({_base_mwh:.2f} MWh) — niet afgetrokken.")

    # ── PV PROFIEL + CLIPPING (v1.5) ──────────────────────────────────────────
    # Volgorde:
    #   1. Vormfactor België × kWp  →  bruto potentieel per kwartier (kW)
    #   2. Cap op omvormer (inverter_kw)  →  AC-clipping
    #   3. Cap op max injectie (aansluiting + lokaal verbruik)  →  grid-clipping
    #      (stap 3 zit impliciet in de LP/passieve dispatch via max_injectie_kw_hard,
    #       maar we passen pv_kw ook expliciet aan zodat KPI's correct zijn)

    _pv_kwp       = inp['pv'].get('kwp', 0)
    _inv_tabel    = {125: 96, 150: 115, 200: 153}
    _pv_inv_kw    = inp['pv'].get('inverter_kw',
                    _inv_tabel.get(_pv_kwp, round(_pv_kwp * 0.77)) if _pv_kwp > 0 else 0)
    _max_inj_kw   = inp['aansluiting'].get('max_injectie_kw_hard',
                    inp['aansluiting'].get('max_injectie_kw_zacht', 1e9))
    _max_afn_kw   = inp['aansluiting'].get('max_afname_kw_hard', 80)
    _edge_case_waarschuwing = None

    # Stap 1: bouw bruto PV-profiel op basis van vormfactor
    pv_kw = build_pv_profile(
        inp['pv'].get('vorm_kwartier', []),
        _pv_kwp,
        inp['pv'].get('specifiek_rendement_kwh_per_kwp', 900),
        sim_timestamps,
    )

    if _pv_kwp > 0 and max(pv_kw) > 0:
        _mwh_bruto = sum(pv_kw) * 0.25 / 1000

        # Stap 2: omvormer-clipping (AC-vermogen limiet)
        if _pv_inv_kw and max(pv_kw) > _pv_inv_kw:
            pv_kw = [min(p, _pv_inv_kw) for p in pv_kw]
            _clip_inv = _mwh_bruto - sum(pv_kw) * 0.25 / 1000
            log.info(f"PV: omvormer-clipping {_clip_inv:.2f} MWh (cap {_pv_inv_kw:.0f} kW)")

        # Stap 3: grid-clipping op max injectie + lokaal verbruik
        # Alleen toepassen zonder batterij — met batterij handelt de LP dit intern af
        _mwh_na_inv = sum(pv_kw) * 0.25 / 1000
        _heeft_batterij = inp['batterij'].get('kwh', 0) > 0
        if not _heeft_batterij:
            pv_kw = [min(pv_kw[i], _max_inj_kw + consumption_kw[i]) for i in range(N)]
        _clip_grid = _mwh_na_inv - sum(pv_kw) * 0.25 / 1000
        if _clip_grid > 0.01:
            log.info(f"PV: grid-clipping {_clip_grid:.2f} MWh "
                     f"(injectie-cap {_max_inj_kw:.0f} kW + lokaal verbruik)")

        _mwh_netto = sum(pv_kw) * 0.25 / 1000
        log.info(f"PV: max={max(pv_kw):.1f} kW, netto={_mwh_netto:.1f} MWh "
                 f"(bruto={_mwh_bruto:.1f}, inv-clip={_mwh_bruto-_mwh_na_inv:.1f}, "
                 f"grid-clip={_clip_grid:.1f})")

        # Waarschuwing: als omvormer groter is dan aansluiting zonder voldoende BESS-buffer
        _batt_kw   = inp['batterij'].get('kw', 0)
        _pv_ratio  = _pv_inv_kw / _max_afn_kw if _max_afn_kw > 0 else 0
        _bess_ratio= _batt_kw   / _max_afn_kw if _max_afn_kw > 0 else 0
        if _pv_ratio > 1.5 and _bess_ratio < 0.3:
            _edge_case_waarschuwing = (
                f"Let op: PV-omvormer ({_pv_inv_kw:.0f} kW) is {_pv_ratio:.1f}\u00d7 groter "
                f"dan het toegangsvermogen ({_max_afn_kw:.0f} kW) "
                f"zonder voldoende batterijbuffer ({_batt_kw:.0f} kW). "
                f"Simulator kan tot ~6% afwijken — gebruik met voorzichtigheid."
            )
            log.warning(f"WAARSCHUWING: {_edge_case_waarschuwing}")
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

    # ── LAADPLEINEN (v1.8): flexibele EV-laadlast toevoegen ──────────────────
    # Modus volgt de 3-sturingen-variant: geen_arbitrage → onmiddellijk (dom laden);
    # bsp actief → shift op onbalans; anders → shift op spot + PV-zelfconsumptie.
    _ev_load = [0.0] * N
    _ev_mwh = 0.0
    _lp_cap = None
    _conn_verhoogd_kw = 0.0   # v1.8.10: auto-verhoging aansluiting bij te kleine batterij
    _conn_hard = inp['aansluiting'].get('max_afname_kw_hard', 1e12)
    if _lp_prep['pleinen']:
        # Capaciteits-check + dimensionering (t.o.v. het HUIDIGE toegangsvermogen, zonder batterij):
        # past de laadvraag binnen het venster onder de aansluiting? Zo niet → verhoging/batterij-advies.
        _lp_cap = _laadplein_capaciteit(_lp_prep, sim_timestamps, spot_actual, imb_actual,
                                        pv_kw, consumption_kw, _conn_hard)
        if inp.get('geen_arbitrage', False):
            # v1.8.5: variant 1 = GEEN sturing. EV laadt onmiddellijk en wordt
            # NIET getemperd door het toegangsvermogen (batterij idle) → de totale
            # afname mag boven het toegangsvermogen uitkomen. Dit toont de
            # overschrijding die je zonder sturing zou dragen (referentie).
            _ev_modus = 'onmiddellijk'
            _ev_conn = 1e12
            _batt_kw = 0.0
        else:
            # Variant 2 & 3: connection-aware laden — de totale afname (verbruik +
            # EV + batterij) blijft ONDER het toegangsvermogen (batterij buffert).
            _ev_modus = 'shift_onbalans' if inp.get('bsp', {}).get('actief', False) else 'shift_spot'
            _ev_conn = _conn_hard
            _batt_kw = inp['batterij'].get('kw', 0) or 0
        _ev_load, _ev_tekort = _bouw_ev_load(_lp_prep, sim_timestamps, spot_actual, imb_actual,
                                             pv_kw, consumption_kw, _ev_modus,
                                             connection_kw=_ev_conn, battery_kw=_batt_kw)
        # v1.8.10: MANUELE BATTERIJ ONTOEREIKEND (variant 2/3) → verhoog het
        # toegangsvermogen tot de laadvraag haalbaar wordt, i.p.v. dagen te
        # verliezen. (Variant 1 laadt ongetemperd op 1e12 → geen tekort, geen raise.)
        _conn_verhoogd_kw = 0.0
        if (not inp.get('geen_arbitrage', False)) and _ev_tekort > 1e-6 and _ev_conn < 1e11:
            _lo = _ev_conn
            _hi = _ev_conn + _lp_prep['tot_cap_kw'] + (max(consumption_kw) if consumption_kw else 0.0) + 1.0
            for _ in range(28):
                _mid = (_lo + _hi) / 2.0
                _, _tk = _bouw_ev_load(_lp_prep, sim_timestamps, spot_actual, imb_actual,
                                       pv_kw, consumption_kw, _ev_modus,
                                       connection_kw=_mid, battery_kw=_batt_kw)
                if _tk <= 1e-6:
                    _hi = _mid
                else:
                    _lo = _mid
            _conn_verhoogd_kw = round(_hi - _ev_conn, 1)
            _ev_conn = _hi
            _conn_hard = _hi
            # Downstream (dispatch + factuur) gebruikt de verhoogde aansluiting.
            inp['aansluiting']['max_afname_kw_hard'] = _hi
            if inp['aansluiting'].get('max_afname_kw_zacht', 0) < _hi:
                inp['aansluiting']['max_afname_kw_zacht'] = _hi
            _ev_load, _ev_tekort = _bouw_ev_load(_lp_prep, sim_timestamps, spot_actual, imb_actual,
                                                 pv_kw, consumption_kw, _ev_modus,
                                                 connection_kw=_ev_conn, battery_kw=_batt_kw)
            _w = (f"Manuele batterij ontoereikend voor de laadvraag → toegangsvermogen "
                  f"verhoogd met ~{_conn_verhoogd_kw:.0f} kW (naar {_hi:.0f} kW) zodat alles "
                  f"laadt. Overweeg een grotere batterij om die verhoging te vermijden.")
            _edge_case_waarschuwing = (_edge_case_waarschuwing + ' ' + _w) if _edge_case_waarschuwing else _w
            log.warning(_w)
        _ev_mwh = sum(_ev_load) * 0.25 / 1000.0
        consumption_kw = [consumption_kw[i] + _ev_load[i] for i in range(N)]
        log.info(
            f"Laadpleinen: EV-last +{_ev_mwh:.2f} MWh (modus={_ev_modus}, piek {max(_ev_load):.0f} kW, "
            f"cap {_lp_prep['tot_cap_kw']:.0f} kW). Capaciteit: "
            + ('OK' if _lp_cap['voldoende'] else
               f"TEKORT — verhoging {_lp_cap['verhoging_kw']:.0f} kW OF batterij "
               f"{_lp_cap['advies_batterij_kw']:.0f} kW / {_lp_cap['advies_batterij_kwh']:.0f} kWh")
        )
        if not _lp_cap['voldoende']:
            _w = (f"Toegangsvermogen ontoereikend voor de laadvraag: {_lp_cap['tekort_mwh']:.2f} MWh "
                  f"raakt niet geladen in het venster. Verhoog het toegangsvermogen met "
                  f"~{_lp_cap['verhoging_kw']:.0f} kW, OF voorzie een batterij van "
                  f"~{_lp_cap['advies_batterij_kw']:.0f} kW / {_lp_cap['advies_batterij_kwh']:.0f} kWh.")
            _edge_case_waarschuwing = (_edge_case_waarschuwing + ' ' + _w) if _edge_case_waarschuwing else _w
            log.warning(_w)

    # ---- PV-curtailment (v1.3) ----
    # Strategie: cap PV op eigen verbruik wanneer DAM (spot) onder een ingestelde drempel zakt.
    # Trigger: spot_actual[i] < trigger_eur_mwh
    # Effect:  pv_kw[i] = min(pv_kw[i], consumption_kw[i])  → injectie wordt 0, eigen verbruik blijft
    pv_curt_cfg = inp.get('pv_curtailment', {})
    pv_curtailed_kwh = 0.0          # totaal verloren PV-productie (kWh)
    pv_curtailed_kwartieren = 0     # # kwartieren waarin curtailment actief was
    vermeden_kost_eur = 0.0         # wat injectie zonder curtailment zou hebben gekost (positief = besparing)
    pv_kw_origineel = list(pv_kw)   # bewaar zonder curtailment voor KPI

    if pv_curt_cfg.get('actief', False) and max(pv_kw) > 0:
        trigger = pv_curt_cfg.get('trigger_eur_mwh', 0.0)
        log.info(f"PV-curtailment actief (cap op verbruik) — trigger: spot < {trigger} €/MWh")
        for i in range(N):
            if spot_actual[i] < trigger:
                potentiele_injectie_kw = pv_kw[i] - consumption_kw[i]  # wat ZOU geïnjecteerd worden
                if potentiele_injectie_kw > 0:
                    # Cap op eigen verbruik: PV beperkt tot wat we zelf gebruiken
                    pv_kw[i] = consumption_kw[i]
                    pv_curtailed_kwh += potentiele_injectie_kw * 0.25
                    pv_curtailed_kwartieren += 1
                    # Vermeden kost: deze MWh zou tegen spot zijn geïnjecteerd
                    # Bij negatieve spot is dat een KOST → vermeden = positief
                    # injectie_kost = - injectie_kWh × spot/1000 (uit factuurlogica A8)
                    # Bij negatieve spot: -kWh × negative = positief (kost)
                    # Door curtailment vermijden we deze kost
                    vermeden_kost_eur += potentiele_injectie_kw * 0.25 * (-spot_actual[i]) / 1000.0
        if pv_curtailed_kwartieren > 0:
            log.info(f"  Curtailment toegepast op {pv_curtailed_kwartieren} kwartieren ({pv_curtailed_kwh/1000:.2f} MWh verloren productie, vermeden kost ~€{vermeden_kost_eur:.0f})")
        else:
            log.info("  Geen curtailment-momenten in deze periode (spot bleef altijd boven drempel)")

    # ---- Forecasts (D-1) ----
    sf = inp.get('forecast', {})
    spot_forecast = add_gaussian_noise(spot_actual, sf.get('sigma_da', 0), rng)
    # v1.7.1: sturing-modus 'geen arbitrage' (variant 1 van de 3-sturingen-KPI).
    # De batterij mag WEL zelfconsumptie + piekshaving doen, maar GEEN spot/IMB-
    # arbitrage. We voeden de LP een VLAKKE dispatch-prijs (= gemiddelde spot),
    # zodat er geen tijdsarbitrage-prikkel is. De FACTURATIE blijft op de echte
    # spot (spot_actual) rekenen. Zelfconsumptie blijft geprikkeld door de
    # markup/markdown-spread; piekshaving door de overschrijdings-penalty.
    geen_arbitrage = bool(inp.get('geen_arbitrage', False))
    if geen_arbitrage and spot_actual:
        _vlak = sum(spot_actual) / len(spot_actual)
        spot_forecast = [_vlak] * N
        log.info(f"geen_arbitrage-modus: vlakke dispatch-prijs {_vlak:.2f} €/MWh (enkel zelfconsumptie + piekshaving)")
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
    nom_grid_in_all  = []  # v1.5 stacked: DAM-nominatie afname
    nom_grid_out_all = []  # v1.5 stacked: DAM-nominatie injectie
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
    
    # === BSP CONFIG ===
    # Optie E: forecast-met-ruis als nominatie + flex
    bsp_cfg = inp.get('bsp', {})
    bsp_actief = bsp_cfg.get('actief', False)
    stacked_modus = bsp_cfg.get('stacked', False) and batt.get('kwh', 0) > 0 and not bsp_actief
    # v1.7.1: in geen_arbitrage-modus dwingen we de plain lp_dispatch_day af
    # (geen stacked DAM+IMB-arbitrage, geen BSP). Samen met de vlakke dispatch-
    # prijs hierboven levert dit een batterij die enkel zelfconsumeert + piekshaaft.
    if geen_arbitrage:
        stacked_modus = False
        bsp_actief = False
    
    # === SNELLE PAD 1: geen batterij EN geen PV → geen LP nodig ===
    skip_lp = (batt['kwh'] <= 0 or batt['kw'] <= 0) and (max(pv_kw) if pv_kw else 0) <= 0
    # === SNELLE PAD 2: geen batterij MAAR wel PV → geen LP nodig (PV is passief) ===
    pv_only = (batt['kwh'] <= 0 or batt['kw'] <= 0) and (max(pv_kw) if pv_kw else 0) > 0
    
    # BSP-modus heeft altijd LP nodig — overschrijf snelle paden
    if bsp_actief and (max(pv_kw) > 0 or batt['kwh'] > 0):
        skip_lp = False
        pv_only = False

    # Initialiseer BSP-storage (gebruikt zelfs als BSP niet actief, voor consistente structuur)
    nom_dam_kw_all = []
    dev_kw_all = []
    nom_eff_afn_kw_all = []
    nom_eff_inj_kw_all = []
    pv_curtailed_kw_bsp_all = []
    bsp_imb_afname_eur = 0.0
    bsp_imb_injectie_eur = 0.0

    # v1.6 lp_diagnostics tellers (wijziging G)
    lp_optimal_count = 0
    lp_retry1_count = 0
    lp_retry2_count = 0
    lp_verloren_dagen = []  # lijst van ISO-datums waar dag overgeslagen werd
    # v1.7 lp_diagnostics tellers maand-niveau (BSP-modus draait nu per maand)
    lp_optimal_maanden = 0
    lp_retry1_maanden = 0
    lp_retry2_maanden = 0
    lp_feasibility_maanden = []  # v1.8: maanden opgelost via haalbaar-dispatch (fysieke cap overschreden)
    lp_verloren_maanden = []  # lijst van YYYY-MM strings waar maand overgeslagen werd
    
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
    elif pv_only or geen_arbitrage:
        # v1.8.5: variant 1 (geen_arbitrage) = GEEN sturing → batterij idle,
        # zuiver passieve dispatch. grid_in = max(verbruik − PV, 0) mag boven het
        # toegangsvermogen (overschrijding wordt geregistreerd + gefactureerd),
        # want zonder sturing wordt er niets afgevlakt.
        log.info("Passieve dispatch (geen sturing / PV zonder batterij) — batterij idle")
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
        # === STANDAARD LP-PAD (batterij of BSP-modus) ===
        if bsp_actief:
            # BSP-modus v1.7: gebruik lp_dispatch_month_bsp (één LP per kalendermaand).
            # Voegt Groep B/D capaciteit-kost toe aan objective via monthly_peak-variabele,
            # zodat LP economisch optimale balans kiest tussen BSP-arbitrage-winst en
            # maandpiek-kost. SoC casadeert nu per kalendermaand (was: per dag).
            log.info("BSP-modus actief (v1.7 maand-LP) — Groep B-kost in objective via monthly_peak")
            pv_curt_allowed = bsp_cfg.get('pv_curtailment_allowed', True)
            # Capture rate: hoeveel paper-deviation toelaten per dag als % van werkelijk dagvolume.
            # Default 1.8% = gekalibreerd tegen externe simulator op rolling 12m slager 200MWh + 125 kWp.
            # In productie kan deze worden bijgesteld via 'forecast_modus' (conservatief/realistic/optimistisch).
            forecast_modus = bsp_cfg.get('forecast_modus', 'realistic')
            modus_multiplier = {
                'conservatief': 0.67,   # -33% van realistic
                'realistic': 1.0,
                'optimistisch': 1.5,    # +50% van realistic
            }.get(forecast_modus, 1.0)
            paper_capture_rate = bsp_cfg.get('paper_capture_rate', 0.018) * modus_multiplier
            log.info(f"BSP capture rate: {paper_capture_rate*100:.1f}% ({forecast_modus} × {modus_multiplier})")

            # v1.7: bepaal kalendermaand-grenzen op basis van sim_timestamps.
            # Een "maand-blok" is een aaneengesloten reeks kwartieren waarvan
            # alle ts.month gelijk zijn (zodat een maand-grens overgang in januari
            # van het volgende jaar ook correct wordt herkend).
            maand_grenzen = []  # lijst van (i0, i1, year, month)
            if sim_timestamps:
                _start = 0
                _cur_year = sim_timestamps[0].year
                _cur_month = sim_timestamps[0].month
                for _i, _ts in enumerate(sim_timestamps):
                    if _ts.year != _cur_year or _ts.month != _cur_month:
                        maand_grenzen.append((_start, _i, _cur_year, _cur_month))
                        _start = _i
                        _cur_year = _ts.year
                        _cur_month = _ts.month
                maand_grenzen.append((_start, N, _cur_year, _cur_month))
            n_maanden_bsp = len(maand_grenzen)
            log.info(f"BSP: {n_maanden_bsp} kalendermaand-blokken te verwerken")

            _netbeheer_tarieven = inp.get('netbeheer', {}).get('tarieven', {})

            for _midx, (i0, i1, _yr, _mo) in enumerate(maand_grenzen):
                _H_maand = i1 - i0
                if _H_maand <= 0:
                    continue
                _n_dagen_maand = max(1, _H_maand // 96)

                result = lp_dispatch_month_bsp(
                    consumption_kw=consumption_kw[i0:i1],
                    pv_kw=pv_kw[i0:i1],
                    spot_eur_mwh=spot_actual[i0:i1],   # DAM perfect (D-1 bekend)
                    imb_eur_mwh=imb_actual[i0:i1],     # IMB PERFECT FORESIGHT
                    soc_start_kwh=soc_kwh,
                    batterij=batt,
                    aansluiting=inp['aansluiting'],
                    contract=inp['contract'],
                    cyclus_kost_eur_per_kwh=cyclus_kost,
                    paper_capture_rate=paper_capture_rate,
                    pv_curtailment_allowed=pv_curt_allowed,
                    netbeheer_tarieven=_netbeheer_tarieven,
                    consumption_forecast_kw=consumption_forecast[i0:i1],
                    pv_forecast_kw=pv_forecast[i0:i1],
                )

                # v1.7 lp_diagnostics: maand-niveau tellers + dagen-aggregaat
                _retry = result.get('retry_level', 0)
                _maand_label = f"{_yr:04d}-{_mo:02d}"
                if _retry == 0:
                    lp_optimal_maanden += 1
                    lp_optimal_count += _n_dagen_maand
                elif _retry == 1:
                    lp_retry1_maanden += 1
                    lp_retry1_count += _n_dagen_maand
                elif _retry == 2:
                    lp_retry2_maanden += 1
                    lp_retry2_count += _n_dagen_maand
                elif _retry == 3:
                    # v1.8 Niveau 3: maand opgelost via HAALBAAR dispatch (de last
                    # wordt bediend; fysieke aansluiting is overschreden). Maand is
                    # NIET verloren — de overschrijding komt via over_afn_hard naar
                    # boven als waarschuwing.
                    lp_feasibility_maanden.append(_maand_label)
                else:
                    # Niveau 4 (noodval): maand overgeslagen — alle dagen verloren
                    lp_verloren_maanden.append(_maand_label)
                    for _d in range(_n_dagen_maand):
                        _idx = i0 + _d * 96
                        if _idx < N:
                            lp_verloren_dagen.append(sim_timestamps[_idx].date().isoformat())

                grid_in_all.extend(result['grid_in'])
                grid_out_all.extend(result['grid_out'])
                p_dis_all.extend(result['p_discharge'])
                p_ch_all.extend(result['p_charge'])
                soc_all.extend(result['soc'][1:])
                # SoC-cascade sanity check (v1.6 wijziging F, behouden in v1.7)
                _new_soc = result['soc'][-1] if result['soc'] else None
                _soc_max = batt['kwh'] * dod
                if _new_soc is None or _new_soc < -0.01 or _new_soc > _soc_max + 0.01:
                    log.warning(
                        f"SoC cascade-defect maand {_maand_label}: soc[-1]={_new_soc}, reset naar 0.5 × soc_max"
                    )
                    _new_soc = _soc_max * 0.5
                soc_kwh = max(0.0, min(_soc_max, _new_soc))
                over_afn_zacht_all.extend(result['over_afn_zacht'])
                over_afn_hard_all.extend(result['over_afn_hard'])
                over_inj_zacht_all.extend(result['over_inj_zacht'])
                over_inj_hard_all.extend(result['over_inj_hard'])
                # BSP-specifieke output
                nom_dam_kw_all.extend(result['nom_dam_kw'])
                dev_kw_all.extend(result['dev_kw'])
                nom_eff_afn_kw_all.extend(result['nom_eff_afn_kw'])
                nom_eff_inj_kw_all.extend(result['nom_eff_inj_kw'])
                pv_curtailed_kw_bsp_all.extend(result['pv_curtailed_kw'])

                _mp = result.get('monthly_peak_kw', 0.0)
                log.info(
                    f"  BSP-maand {_maand_label} ({_n_dagen_maand} dagen) klaar: "
                    f"monthly_peak={_mp:.1f} kW, status={result.get('lp_status', '?')}"
                )
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
                    imb_eur_mwh=None,
                ) if not stacked_modus else lp_dispatch_day_stacked(
                    consumption_forecast[i0:i1],
                    pv_forecast[i0:i1],
                    spot_forecast[i0:i1],
                    imb_actual[i0:i1],
                    soc_kwh,
                    batt,
                    inp['aansluiting'],
                    inp['contract'],
                    cyclus_kost,
                )
                if stacked_modus:
                    nom_grid_in_all.extend(result['nom_grid_in'])
                    nom_grid_out_all.extend(result['nom_grid_out'])
                grid_in_all.extend(result['grid_in'])
                grid_out_all.extend(result['grid_out'])
                p_dis_all.extend(result['p_discharge'])
                p_ch_all.extend(result['p_charge'])
                soc_all.extend(result['soc'][1:])
                # v1.6 wijziging F: SoC-cascade sanity check
                _new_soc = result['soc'][-1] if result['soc'] else None
                _soc_max = batt['kwh'] * dod
                if _new_soc is None or _new_soc < -0.01 or _new_soc > _soc_max + 0.01:
                    log.warning(
                        f"SoC cascade-defect dag {d}: soc[-1]={_new_soc}, reset naar 0.5 × soc_max"
                    )
                    _new_soc = _soc_max * 0.5
                soc_kwh = max(0.0, min(_soc_max, _new_soc))
                # v1.6 wijziging G: lp_diagnostics teller. lp_dispatch_day en
                # _stacked hebben geen retry-ladder, dus elke niet-Optimal-dag
                # zit in een eigen verloren_dagen-bucket-equivalent (we
                # registreren ze als verloren wanneer status != Optimal).
                _lp_status = result.get('lp_status', 'Optimal')
                if _lp_status == 'Optimal':
                    lp_optimal_count += 1
                else:
                    lp_verloren_dagen.append(sim_timestamps[i0].date().isoformat())
                over_afn_zacht_all.extend(result['over_afn_zacht'])
                over_afn_hard_all.extend(result['over_afn_hard'])
                over_inj_zacht_all.extend(result['over_inj_zacht'])
                over_inj_hard_all.extend(result['over_inj_hard'])

                if (d + 1) % 30 == 0:
                    log.info(f"  LP-dispatch dag {d+1}/{n_dagen}…")

    log.info(
        f"LP-dispatch klaar: {n_dagen} dagen, eind-SoC = {soc_kwh:.1f} kWh "
        f"(optimal={lp_optimal_count}, retry1={lp_retry1_count}, "
        f"retry2={lp_retry2_count}, verloren={len(lp_verloren_dagen)}) "
        f"| maand-niveau (BSP): optimal_m={lp_optimal_maanden}, retry1_m={lp_retry1_maanden}, "
        f"retry2_m={lp_retry2_maanden}, feasibility_m={len(lp_feasibility_maanden)}, "
        f"verloren_m={len(lp_verloren_maanden)}"
    )

    # ---- Werkelijke factuur (met werkelijke spot/imb, niet forecast) ----
    log.info("Jaarfactuur berekenen…")
    contract_for_factuur = dict(inp['contract'])
    
    # Bouw nominatie-arrays voor BSP-modus.
    # Optie E zonder ruis (eerste versie): nom_afn = max(consumption - pv, 0), nom_inj = max(pv - consumption, 0)
    # Dat is de "verwachte fysieke positie" zonder LP-flex. LP heeft consumption/pv niet aangepast,
    # alleen via curtail/batterij. Dus nominatie ≠ werkelijk netto-volume.
    # Sigma-ruis op consumption/pv komt in milestone 2C.
    nom_afn_arr = None
    nom_inj_arr = None
    # v1.5 Stacked: nominatie = DAM-geoptimaliseerde dispatch
    # Deviatie = werkelijk (IMB) - nominatie (DAM) → IMB-spread gecapteerd
    # v1.5 Stacked: IMB-factor automatisch gekalibreerd op batterijvermogen/aansluiting
    # factor = 1.0984 - 0.328 * (batt_kw / max_afname_kw)
    # Gekalibreerd op ref: bess50 (+3.3%), bess100 (-3.1%), bess200 (~0%)
    _batt_kw = batt.get('kw', 0)
    _aansluiting_kw = inp['aansluiting'].get('max_afname_kw_hard', 80.0)
    _ratio = _batt_kw / _aansluiting_kw if _aansluiting_kw > 0 else 0.0
    # Kwadratische kalibratie op ref (bess50/100/200 met ref capex):
    _default_factor = max(0.5, min(1.0, 1.4038 - 1.5732 * _ratio + 0.8731 * _ratio ** 2))
    stacked_imb_factor = bsp_cfg.get('stacked_imb_factor', _default_factor)
    if stacked_modus:
        log.info(f"  Stacked IMB factor: {stacked_imb_factor:.3f} "
                 f"(ratio={_ratio:.2f}, auto={_default_factor:.3f})")
    imb_actual_stacked = [
        spot_actual[i] + stacked_imb_factor * (imb_actual[i] - spot_actual[i])
        for i in range(N)
    ] if stacked_imb_factor != 1.0 else imb_actual
    if stacked_modus and nom_grid_in_all:
        nom_afn_arr = nom_grid_in_all
        nom_inj_arr = nom_grid_out_all
        log.info(f"  Stacked nominatie (DAM-LP): "
                 f"{sum(nom_afn_arr)*0.25/1000:.1f} MWh afname / "
                 f"{sum(nom_inj_arr)*0.25/1000:.1f} MWh injectie")
    if bsp_actief:
        contract_for_factuur['modus'] = 'passthrough'
        # Gebruik effective nominatie uit LP (= forecast + paper-deviation).
        # Dat is wat de ARP daadwerkelijk nomineert na BSP-strategie:
        # forecast als basis + LP-bepaalde papier-dev op gunstige IMB-momenten.
        if nom_eff_afn_kw_all and len(nom_eff_afn_kw_all) == N:
            nom_afn_arr = nom_eff_afn_kw_all
            nom_inj_arr = nom_eff_inj_kw_all
            log.info(f"  BSP eff. nominatie: nom_afn = {sum(nom_afn_arr)*0.25/1000:.1f} MWh, nom_inj = {sum(nom_inj_arr)*0.25/1000:.1f} MWh")
        else:
            # Fallback: forecast-only nominatie
            nom_afn_arr = [max(consumption_forecast[i] - pv_forecast[i], 0) for i in range(N)]
            nom_inj_arr = [max(pv_forecast[i] - consumption_forecast[i], 0) for i in range(N)]
            log.info(f"  BSP forecast nominatie: nom_afn = {sum(nom_afn_arr)*0.25/1000:.1f} MWh, nom_inj = {sum(nom_inj_arr)*0.25/1000:.1f} MWh")

    # v1.6 wijziging J: gebruik heeft_lp_output i.p.v. heeft_batterij.
    # Reden: bij PV-only + BSP zonder BESS draait de LP wél (BSP-modus
    # overschrijft pv_only/skip_lp) maar grid_in/out komen dan uit de
    # LP-variabelen — niet uit een passieve reconstructie. De v1.5-logica
    # gebruikte enkel heeft_batterij waardoor de LP-output (incl pv_curt)
    # genegeerd werd voor PV+BSP-zonder-BESS scenarios.
    heeft_lp_output = not skip_lp
    if heeft_lp_output:
        # LP heeft fysieke stromen bepaald (incl pv_curt, batterij, BSP-flex)
        fysiek_grid_in  = list(grid_in_all)
        fysiek_grid_out = list(grid_out_all)
    else:
        # Echt passieve fallback (geen LP gedraaid: skip_lp pad)
        fysiek_grid_in  = [max(0.0, consumption_kw[i] - pv_kw[i]) for i in range(N)]
        fysiek_grid_out = [max(0.0, pv_kw[i] - consumption_kw[i]) for i in range(N)]

    factuur = bereken_jaarfactuur(
        fysiek_grid_in,
        fysiek_grid_out,
        spot_actual,
        # IMB voor factuur: stacked gebruikt gefilterde IMB, BSP ook indien factor ingesteld
        imb_actual_stacked if (stacked_modus or bsp_actief) else imb_actual,
        sim_timestamps,
        contract_for_factuur,
        inp['netbeheer'],
        n_maanden=n_maanden_voor_factuur,
        nom_afn_kw_all=nom_afn_arr,
        nom_inj_kw_all=nom_inj_arr,
        effectief_jaarverbruik_voor_pricing=effectief_jaarverbruik_mwh,
        aansluiting=inp.get('aansluiting'),  # v1.6 wijziging K
    )

    # ---- KPI's ----
    pv_naar_eigen_verbruik = sum(min(consumption_kw[i], pv_kw[i]) * 0.25 / 1000.0 for i in range(N))
    totaal_verbruik_mwh = sum(consumption_kw) * 0.25 / 1000.0
    totaal_pv_mwh = sum(pv_kw) * 0.25 / 1000.0
    # Industriestandaard (v1.2: hersteld na verwisseling in v1.1):
    #   Zelfconsumptie  = % van PV-productie dat zelf wordt verbruikt (rest = injectie)
    #   Zelfvoorziening = % van verbruik dat door eigen PV wordt gedekt (rest = afname)
    pct_zelfconsumptie = (pv_naar_eigen_verbruik / totaal_pv_mwh * 100) if totaal_pv_mwh > 0 else 0
    pct_zelfvoorziening = (pv_naar_eigen_verbruik / totaal_verbruik_mwh * 100) if totaal_verbruik_mwh > 0 else 0

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
        # v1.2: expliciete PV-flow-velden zodat UI niet hoeft af te leiden
        'pv_eigen_verbruik_mwh': pv_naar_eigen_verbruik,
        'pv_injectie_mwh': max(0.0, totaal_pv_mwh - pv_naar_eigen_verbruik),
        # v1.3: curtailment KPIs
        'pv_curtailed_mwh': pv_curtailed_kwh / 1000.0,
        'pv_curtailed_kwartieren': pv_curtailed_kwartieren,
        'pv_potentiele_productie_mwh': sum(pv_kw_origineel) * 0.25 / 1000.0,
        'vermeden_injectie_kost_eur': vermeden_kost_eur,
        # Fysieke netto grid-flows:
        # - Zonder batterij: consumption - pv geeft exacte fysieke stromen
        # - Met batterij: LP-variabelen zijn de fysieke stromen (batterij wijzigt grid_in/out)
        'totaal_grid_in_mwh': (
            sum(max(0.0, consumption_kw[i] - pv_kw[i]) for i in range(N)) * 0.25 / 1000.0
            if batt['kwh'] <= 0
            else sum(max(0.0, grid_in_all[i] - grid_out_all[i]) for i in range(N)) * 0.25 / 1000.0
        ),
        'totaal_grid_out_mwh': (
            sum(max(0.0, pv_kw[i] - consumption_kw[i]) for i in range(N)) * 0.25 / 1000.0
            if batt['kwh'] <= 0
            else sum(max(0.0, grid_out_all[i] - grid_in_all[i]) for i in range(N)) * 0.25 / 1000.0
        ),
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
            # v1.6 wijziging L: nieuwe velden
            'jaarpiek_injectie_kw': factuur['jaarpiek_injectie_kw'],
            'winterpiek_afname_kw': factuur['winterpiek_afname_kw'],
        },
        'kpi': kpi,
        'waarschuwing': _edge_case_waarschuwing,
        'laadplein': {
            'aantal_pleinen': len(_lp_prep['pleinen']),
            'ev_last_mwh': round(_ev_mwh, 3),
            'bestaand_afgetrokken_mwh': round(_lp_prep['bestaand_periode_mwh'], 3),
            'nieuw_mwh': round(_lp_prep['nieuw_periode_mwh'], 3),
            'totaal_laadvermogen_kw': round(_lp_prep['tot_cap_kw'], 1),
            'ondergrens_batterij_kw': round(_lp_prep['tot_cap_kw'], 1),
            'piek_ev_kw': round(max(_ev_load) if _ev_load else 0.0, 1),
            # v1.8.10: automatische aansluitingsverhoging omdat de (manuele) batterij
            # ontoereikend was (0 = geen verhoging toegepast).
            'toegangsvermogen_verhoogd_kw': _conn_verhoogd_kw,
            # v1.8: capaciteits-check + dimensionering (opstelling 1 = verhoging, opstelling 2 = batterij).
            'capaciteit': _lp_cap,
        },
        'maandstaten': maandstaten,
        'soc_reeks': soc_all[:N],  # cap to N
        # v1.8.11: per-kwartier-arrays voor het financieel rapport (heatmaps).
        # Compact afgerond. vermogen>0 = afname, <0 = injectie. kost = all-in €/MWh
        # (afname: spot+markup+belastingen ; injectie: −(spot−markdown), = inkomst).
        'profielen': (lambda _mk=(inp['contract'].get('markup_eur_mwh',0) or 0),
                             _md=(inp['contract'].get('markdown_eur_mwh',0) or 0),
                             _bel=((inp['contract'].get('gsc_eur_mwh',0) or 0)
                                   +(inp['contract'].get('wkk_eur_mwh',0) or 0)
                                   +(inp['contract'].get('vergroening_eur_per_mwh',0) or 0)): {
            'n': N,
            'van': sim_timestamps[0].isoformat() if N else None,
            'vermogen_aansluiting_kw': [round(grid_in_all[i] - grid_out_all[i], 1) for i in range(N)],
            'spot_prijs_eur_mwh': [round(spot_actual[i], 1) for i in range(N)],
            'kost_eur_mwh': [
                round((spot_actual[i] + _mk + _bel) if (grid_in_all[i] - grid_out_all[i]) >= 0
                      else -(spot_actual[i] - _md), 1)
                for i in range(N)
            ],
        })(),
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
        # v1.6 wijziging G+L: LP-diagnostics top-level. Geeft UI een groen/geel/rood
        # badge zodat advisors weten of LP-oplos-rate gezond is voor deze scenario.
        # v1.7: maand-niveau velden toegevoegd (BSP-modus draait nu per maand);
        # bestaande dag-niveau velden blijven gevuld als aggregaat (dagen in maanden)
        # voor backwards-compat met Simulator.txt v1.18 badge-logica.
        'lp_diagnostics': {
            'totaal_dagen': n_dagen,
            'optimal_dagen': lp_optimal_count,
            'retry1_dagen': lp_retry1_count,
            'retry2_dagen': lp_retry2_count,
            'verloren_dagen': lp_verloren_dagen,
            # v1.7 maand-niveau (alleen gevuld in BSP-modus; anders 0/[])
            'totaal_maanden': (
                lp_optimal_maanden + lp_retry1_maanden + lp_retry2_maanden
                + len(lp_feasibility_maanden) + len(lp_verloren_maanden)
            ),
            'optimal_maanden': lp_optimal_maanden,
            'retry1_maanden': lp_retry1_maanden,
            'retry2_maanden': lp_retry2_maanden,
            # v1.8: maanden opgelost via haalbaar-dispatch (fysieke aansluiting
            # bewust overschreden om last te bedienen — prioriteit haalbaarheid)
            'feasibility_maanden': lp_feasibility_maanden,
            'verloren_maanden': lp_verloren_maanden,
        },
        'data_periode': {
            'van': sim_timestamps[0].isoformat(),
            'tot': sim_timestamps[-1].isoformat(),
            'aantal_kwartieren': N,
            # v1.5 sessie 4: diagnose voor base-case-flow + KPI-tegel
            'type': periode_type,
            'is_specifiek': is_specifiek,
            'n_kalendermaanden': n_maanden_voor_factuur,
            'effectief_jaarverbruik_mwh': effectief_jaarverbruik_mwh,
            'periode_volume_mwh': inp['jaarverbruik_mwh'] if is_specifiek else None,
        },
    }

    log.info("=== Simulator klaar ===")
    return output


# =============================================================================
# BATTERIJ-ARBITRAGE ANALYSE (v1.5 — M1/M2/M3)
# Heuristic, geen LP. Snel (<1s). Sales-tool.
# =============================================================================

def _build_full_profiles(inp: dict, sim_timestamps: list) -> tuple:
    """
    Bouw consumption_kw en pv_kw arrays voor de sim-periode.
    Hergebruikt build_consumption_profile en build_pv_profile.
    Returns: (consumption_kw[N], pv_kw[N])
    """
    N = len(sim_timestamps)

    # Verbruiksprofiel
    basis_profiel = inp.get('profiel_kwartier') or inp.get('profiel_voorbeeld_eerste_24u')
    if basis_profiel and len(basis_profiel) < 35040:
        # Uitgebreide voorbeeld-input: herhaal dagprofiel tot 35040 kwartieren
        dag_len = 96
        if len(basis_profiel) <= dag_len:
            herhaal = (35040 // dag_len) + 1
            basis_profiel = (basis_profiel * herhaal)[:35040]
        # Renormaliseer
        s = sum(basis_profiel)
        if s > 0:
            basis_profiel = [v / s for v in basis_profiel]

    aanvullingen = inp.get('aanvullingen') or {}
    consumption_kw = build_consumption_profile(
        basis_profiel, inp['jaarverbruik_mwh'], aanvullingen, sim_timestamps
    )

    # PV-profiel
    pv_conf = inp.get('pv') or {}
    kwp = float(pv_conf.get('kwp', 0))
    spec_rend = float(pv_conf.get('specifiek_rendement_kwh_per_kwp', 950))
    pv_vorm = pv_conf.get('vorm_kwartier') or pv_conf.get('vorm_voorbeeld_juni_dag') or []
    pv_kw = build_pv_profile(pv_vorm, kwp, spec_rend, sim_timestamps)

    return consumption_kw, pv_kw


def _get_markt_arrays(inp: dict, N: int) -> tuple:
    """
    Haal DAM en IMB arrays op uit inp['markt'] of genereer dummy-data.
    Returns: (dam_eur_mwh[N], imb_eur_mwh[N])
    Beide arrays hebben lengte N.
    """
    markt = inp.get('markt') or {}
    dam_raw = markt.get('spot_kwartier') or []
    imb_raw = markt.get('imb_kwartier') or []

    # Lengte aanpassen/herhalen indien nodig
    def _fit(arr, n):
        if not arr:
            return None
        if len(arr) >= n:
            return list(arr[:n])
        # Herhaal circulair
        out = []
        while len(out) < n:
            out.extend(arr)
        return out[:n]

    dam = _fit(dam_raw, N)
    imb = _fit(imb_raw, N)

    # Fallback: Belgisch seizoens-gemiddelde als geen data
    if dam is None:
        import math as _math
        dam = []
        for i in range(N):
            uur = (i % 96) / 4.0
            dag_type = (i // 96) % 7
            seizoen = _math.sin(2 * _math.pi * i / (96 * 365)) * 25
            dag_cyclus = 40 + 30 * _math.sin(_math.pi * (uur - 6) / 12) * (1 if 6 <= uur <= 22 else 0.3)
            weekend = -10 if dag_type >= 5 else 0
            dam.append(max(-50, dag_cyclus + seizoen + weekend))
    if imb is None:
        imb = [dam[i] + (5 if i % 3 == 0 else -8) for i in range(N)]

    return dam, imb


def _maand_van_index(idx: int, sim_timestamps: list) -> int:
    """Geeft maand (1-12) voor kwartier-index idx."""
    return sim_timestamps[idx].month if idx < len(sim_timestamps) else 1


def _dag_van_index(idx: int) -> int:
    """Geeft dag-nummer (0-based) voor kwartier-index."""
    return idx // 96


# ---- Categorie 1: Maandpiek-shaving ----------------------------------------

def _categorie1_piekshaving(
    consumption_kw: list,
    pv_kw: list,
    sim_timestamps: list,
    batt: dict,
    aansluiting: dict,
    netbeheer: dict,
    cycli_per_jaar: int,
) -> float:
    """
    Berekent jaarlijkse besparing via maandpiek-shaving.
    Returns: besparing_eur_jaar (float), lijst van maandpieken (kw)
    """
    N = len(consumption_kw)
    kw_batt = float(batt['kw'])
    kwh_batt = float(batt['kwh'])
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    kwh_eff = kwh_batt * dod

    tarieven = netbeheer.get('tarieven', {})
    maandpiek_tarief_per_kw_jaar = float(tarieven.get('maandpiek_eur_kw_jaar', 59.76))
    maandpiek_tarief_per_kw_maand = maandpiek_tarief_per_kw_jaar / 12.0

    # Netto afname per kwartier (positief = afname van net)
    netto_afname = [max(0.0, consumption_kw[i] - pv_kw[i]) for i in range(N)]

    # Cyclus-budget per maand in kWh dispatch
    kwh_dispatch_per_dag = cycli_per_jaar * kwh_eff / 365.0
    kwh_budget_per_maand = kwh_dispatch_per_dag * 30.5

    besparing_totaal = 0.0
    maandpieken = {}

    # Groepeer per maand
    maanden = {}
    for i, ts in enumerate(sim_timestamps):
        m = ts.month
        if m not in maanden:
            maanden[m] = []
        maanden[m].append(i)

    for m, indices in sorted(maanden.items()):
        afnames_m = [(netto_afname[i], i) for i in indices]
        if not afnames_m:
            continue
        max_piek = max(v for v, _ in afnames_m)
        maandpieken[m] = max_piek

        # Top-10 pieken sorteren (hoogste eerst)
        afnames_gesorteerd = sorted(afnames_m, key=lambda x: -x[0])
        top10 = afnames_gesorteerd[:10]

        # Bepaal drempel: batterij kan pieken aftoppen
        # Hoeveel kWh nodig om alle top-10 pieken terug te brengen tot drempel?
        # Drempel iteratief zoeken: begin bij piek, verlaag totdat budget op is
        drempel = max_piek
        kwh_ingezet = 0.0
        beschikbaar = min(kwh_budget_per_maand * 0.5, kwh_eff)  # 50% voor shaving (rest voor laden)

        for target_drempel in sorted(set(v for v, _ in top10)):
            extra_kwh = sum(
                max(0, v - target_drempel) * 0.25
                for v, _ in top10
            )
            if extra_kwh <= beschikbaar:
                drempel = target_drempel
                kwh_ingezet = extra_kwh
            else:
                break

        # Hoeveel kW gereduceerd?
        piek_reductie = max(0.0, max_piek - drempel)
        piek_reductie = min(piek_reductie, kw_batt)  # max batterij-vermogen

        besparing_m = piek_reductie * maandpiek_tarief_per_kw_maand
        besparing_totaal += besparing_m

    return besparing_totaal, maandpieken


# ---- Categorie 2: DAM intra-day arbitrage -----------------------------------

def _categorie2_dam_arbitrage(
    dam_eur_mwh: list,
    sim_timestamps: list,
    batt: dict,
    cycli_per_jaar: int,
) -> float:
    """
    Heuristic DAM-arbitrage: per dag, laad bij laagste N kwartieren, ontlaad bij hoogste.
    Chronologie gerespecteerd: laad-kwartieren moeten VOOR ontlaad-kwartieren vallen.
    Returns: revenue_eur_jaar
    """
    N = len(dam_eur_mwh)
    n_dagen = N // 96
    kw_batt = float(batt['kw'])
    kwh_batt = float(batt['kwh'])
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    rte = float(batt.get('rte_pct', 92)) / 100.0 if batt.get('rte_pct', 92) > 1.5 else float(batt.get('rte_pct', 0.92))
    kwh_eff = kwh_batt * dod

    cycli_per_dag = cycli_per_jaar / 365.0
    kwh_per_cyclus = kwh_eff  # één volledige cyclus
    kwh_laad_per_dag = cycli_per_dag * kwh_per_cyclus  # budget laden
    # Max kWh per kwartier via laad/ontlaad-vermogen
    kwh_per_kwartier_max = kw_batt * 0.25

    revenue_totaal = 0.0

    for d in range(n_dagen):
        i0 = d * 96
        i1 = min(i0 + 96, N)
        dag_spot = dam_eur_mwh[i0:i1]
        n_qt = len(dag_spot)
        if n_qt < 4:
            continue

        # Aantal kwartieren voor laden/ontladen
        n_laad_qt = max(1, int(kwh_laad_per_dag / kwh_per_kwartier_max))
        n_laad_qt = min(n_laad_qt, n_qt // 2)

        # Sorteer kwartier-indices op prijs
        gesorteerd_asc = sorted(range(n_qt), key=lambda x: dag_spot[x])
        gesorteerd_desc = sorted(range(n_qt), key=lambda x: -dag_spot[x])

        laad_indices = set(gesorteerd_asc[:n_laad_qt])
        ontlaad_indices = set(gesorteerd_desc[:n_laad_qt])

        # Chronologie: verwijder ontlaad-momenten die VOOR laad-momenten vallen
        # (gebruik mediaan laad-tijdstip als splitsgrens)
        if laad_indices and ontlaad_indices:
            laad_mediaan = sorted(laad_indices)[len(laad_indices) // 2]
            ontlaad_indices = {t for t in ontlaad_indices if t > laad_mediaan}

        if not laad_indices or not ontlaad_indices:
            continue

        laad_prijs = sum(dag_spot[t] for t in laad_indices) / len(laad_indices)
        ontlaad_prijs = sum(dag_spot[t] for t in ontlaad_indices) / len(ontlaad_indices)
        spread = ontlaad_prijs - laad_prijs

        if spread <= 0:
            continue

        # kWh geladen = cycli_per_dag × kwh_per_cyclus (begrensd door budget)
        kwh_geladen = min(kwh_laad_per_dag, n_laad_qt * kwh_per_kwartier_max)
        kwh_ontladen = kwh_geladen * rte

        # Revenue: ontladen × (ontlaadprijs - inkoopkost / rte)
        # Netto: kwh_ontladen × spread / 1000 - kwh_geladen × inkoopkost / 1000
        # Vereenvoudigd: kwh_ontladen × spread_netto / 1000
        revenue_dag = kwh_ontladen * spread / 1000.0

        # Schaal naar dag
        dagen_in_sim = n_dagen
        revenue_totaal += revenue_dag

    # Schaal naar volledig jaar (sim kan korter zijn)
    if n_dagen > 0 and n_dagen < 365:
        revenue_totaal *= 365.0 / n_dagen

    return revenue_totaal


# ---- Categorie 3: PV-curtail-vermijding ------------------------------------

def _categorie3_pv_curtail(
    consumption_kw: list,
    pv_kw: list,
    dam_eur_mwh: list,
    batt: dict,
    cycli_per_jaar: int,
) -> float:
    """
    Kwartieren met DAM<0 én PV>verbruik: batterij absorbeert overschot ipv injectie.
    Returns: revenue_eur_jaar
    """
    N = len(consumption_kw)
    kw_batt = float(batt['kw'])
    kwh_batt = float(batt['kwh'])
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    kwh_eff = kwh_batt * dod

    kwh_dispatch_per_dag = cycli_per_jaar * kwh_eff / 365.0

    # Alle kandidaten: DAM<0 en PV>verbruik
    kandidaten = []
    for i in range(N):
        if dam_eur_mwh[i] < 0 and pv_kw[i] > consumption_kw[i]:
            overschot_kw = pv_kw[i] - consumption_kw[i]
            kan_laden_kw = min(overschot_kw, kw_batt)
            kan_laden_kwh = kan_laden_kw * 0.25
            waarde_eur = kan_laden_kwh * abs(dam_eur_mwh[i]) / 1000.0
            dag = i // 96
            kandidaten.append((i, dag, kan_laden_kwh, waarde_eur))

    # Sorteer op hoogste marginale waarde (EUR per kWh)
    kandidaten.sort(key=lambda x: -(x[3] / x[2] if x[2] > 0 else 0))

    # Alloceer met dagbudget
    budget_per_dag = {}
    revenue_totaal = 0.0

    for idx, dag, kwh_nodig, eur_winst in kandidaten:
        budget_dag = budget_per_dag.get(dag, kwh_dispatch_per_dag * 0.3)  # max 30% budget voor curtail
        if budget_dag >= kwh_nodig:
            revenue_totaal += eur_winst
            budget_per_dag[dag] = budget_dag - kwh_nodig
        elif budget_dag > 0:
            # Gedeeltelijke allocatie
            fractie = budget_dag / kwh_nodig
            revenue_totaal += eur_winst * fractie
            budget_per_dag[dag] = 0.0

    # Schaal naar jaar
    n_dagen = N // 96
    if n_dagen > 0 and n_dagen < 365:
        revenue_totaal *= 365.0 / n_dagen

    return revenue_totaal


# ---- Categorie 4: IMB-arbitrage --------------------------------------------

def _categorie4_imb_arbitrage(
    dam_eur_mwh: list,
    imb_eur_mwh: list,
    batt: dict,
    cycli_per_jaar: int,
    capture_rate: float = 0.018,
) -> float:
    """
    BSP IMB-spread capture: capture_rate × |IMB-DAM| × flex_kWh.
    Returns: revenue_eur_jaar
    """
    N = len(dam_eur_mwh)
    kw_batt = float(batt['kw'])
    kwh_batt = float(batt['kwh'])
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    kwh_eff = kwh_batt * dod

    kwh_dispatch_per_dag = cycli_per_jaar * kwh_eff / 365.0
    flex_kwh_per_kwartier = kw_batt * 0.25  # max flex per kwartier

    # Alle kwartieren met spread > drempel
    DREMPEL_EUR_MWH = 15.0
    kandidaten = []
    for i in range(N):
        spread = abs(imb_eur_mwh[i] - dam_eur_mwh[i])
        if spread > DREMPEL_EUR_MWH:
            kwh = min(flex_kwh_per_kwartier, kwh_eff)
            winst = capture_rate * spread * kwh / 1000.0
            dag = i // 96
            kandidaten.append((i, dag, kwh, winst))

    # Sorteer op marginale waarde
    kandidaten.sort(key=lambda x: -(x[3] / x[2] if x[2] > 0 else 0))

    budget_per_dag = {}
    revenue_totaal = 0.0

    for idx, dag, kwh_nodig, eur_winst in kandidaten:
        budget_dag = budget_per_dag.get(dag, kwh_dispatch_per_dag * 0.4)
        if budget_dag >= kwh_nodig:
            revenue_totaal += eur_winst
            budget_per_dag[dag] = budget_dag - kwh_nodig
        elif budget_dag > 0:
            fractie = budget_dag / kwh_nodig
            revenue_totaal += eur_winst * fractie
            budget_per_dag[dag] = 0.0

    # Schaal naar jaar
    n_dagen = N // 96
    if n_dagen > 0 and n_dagen < 365:
        revenue_totaal *= 365.0 / n_dagen

    return revenue_totaal


# ---- Gecombineerde allocatie met marginale waarde --------------------------

def _alloceer_met_marginale_waarde(
    consumption_kw: list,
    pv_kw: list,
    dam_eur_mwh: list,
    imb_eur_mwh: list,
    sim_timestamps: list,
    batt: dict,
    aansluiting: dict,
    netbeheer: dict,
    cycli_per_jaar: int,
    capture_rate: float = 0.018,
) -> dict:
    """
    Gecorrigeerde allocatie v1.5b:
    - Cat2+Cat4 GECOMBINEERD per dag: gebruik min(DAM,IMB) voor laden,
      max(DAM,IMB) voor ontladen. Dit repliceert de LP-dispatch die zowel
      DAM-arbitrage als IMB-afrekening tegelijk maximaliseert.
    - Cat1: maandpiek-shaving op apart budget (30% van dagtotaal).
    - Cat3: PV-curtail-vermijding op eigen budget (negatieve DAM kwartieren).
    - Cyclus-budget: Cat2+Cat4 deelt 70%, Cat1 krijgt 30%.
    Returns: { piekshaving_eur, dam_arb_eur, pv_curtail_eur, imb_arb_eur, maandpieken_kw }
    """
    N = len(consumption_kw)
    n_dagen = max(1, N // 96)
    kw_batt = float(batt['kw'])
    kwh_batt = float(batt['kwh'])
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    rte = float(batt.get('rte_pct', 92)) / 100.0 if batt.get('rte_pct', 92) > 1.5 else float(batt.get('rte_pct', 0.92))
    kwh_eff = kwh_batt * dod
    kwh_per_qt = kw_batt * 0.25

    kwh_budget_per_dag = cycli_per_jaar * kwh_eff / 365.0
    # Budget-splits: 90% voor arbitrage (Cat2+Cat4), 10% reserve voor piekshaving (Cat1)
    # Gekalibreerd op referentie-simulator: 0.90 geeft ±1% afwijking bij BESS100/714c
    budget_arb_per_dag = kwh_budget_per_dag * 0.90
    budget_piek_per_dag = kwh_budget_per_dag * 0.10

    tarieven = netbeheer.get('tarieven', {})
    maandpiek_tarief_per_kw_jaar = float(tarieven.get('maandpiek_eur_kw_jaar', 59.76))
    maandpiek_tarief_per_kw_maand = maandpiek_tarief_per_kw_jaar / 12.0

    netto_afname = [max(0.0, consumption_kw[i] - pv_kw[i]) for i in range(N)]

    # -----------------------------------------------------------------------
    # Cat 2+4: gecombineerde DAM+IMB arbitrage per dag
    # Laden op min(DAM, IMB), ontladen op max(DAM, IMB) — chronologie bewaard
    # -----------------------------------------------------------------------
    arb_winst = 0.0
    arb_winst_dam = 0.0   # deel toewijsbaar aan DAM-arbitrage (laden/ontladen op DAM)
    arb_winst_imb = 0.0   # deel toewijsbaar aan IMB-premium boven DAM

    for d in range(n_dagen):
        i0 = d * 96
        i1 = min(i0 + 96, N)
        dag_dam = dam_eur_mwh[i0:i1]
        dag_imb = imb_eur_mwh[i0:i1]
        n_qt = len(dag_dam)
        if n_qt < 8:
            continue

        prijs_laden = [min(dag_dam[t], dag_imb[t]) for t in range(n_qt)]
        prijs_ontlaad = [max(dag_dam[t], dag_imb[t]) for t in range(n_qt)]

        # Budget: helft laden, helft ontladen
        kwh_laden_dag = budget_arb_per_dag / 2.0
        kwh_ontlaad_dag = budget_arb_per_dag / 2.0 * rte

        # Sortering: laden op goedkoopste kwartieren, ontladen op duurste
        laden_gs = sorted(range(n_qt), key=lambda t: prijs_laden[t])
        ontlaad_gs = sorted(range(n_qt), key=lambda t: -prijs_ontlaad[t])

        # Chronologie: ontlaad-kwartieren mogen niet tegelijk met laad-kwartieren zijn.
        # Exclusie-filter (geen mediaan-split): geeft ±1% vs referentie (gekalibreerd).
        n_laad_qt = max(1, int(kwh_laden_dag / kwh_per_qt) + 2)
        laden_qt = laden_gs[:min(n_laad_qt, n_qt // 2)]
        laden_set = set(laden_qt)
        ontlaad_qt = [t for t in ontlaad_gs if t not in laden_set]

        if not laden_qt or not ontlaad_qt:
            continue

        b_l, b_o = kwh_laden_dag, kwh_ontlaad_dag
        kosten_laden = 0.0
        kosten_laden_dam = 0.0
        for t in laden_qt:
            q = min(kwh_per_qt, b_l)
            if q < 0.001:
                break
            kosten_laden += prijs_laden[t] * q / 1000.0
            kosten_laden_dam += dag_dam[t] * q / 1000.0
            b_l -= q

        opbrengst_ontlaad = 0.0
        opbrengst_ontlaad_dam = 0.0
        for t in ontlaad_qt:
            q = min(kwh_per_qt, b_o)
            if q < 0.001:
                break
            opbrengst_ontlaad += prijs_ontlaad[t] * q / 1000.0
            opbrengst_ontlaad_dam += dag_dam[t] * q / 1000.0
            b_o -= q

        dag_winst = opbrengst_ontlaad - kosten_laden
        dag_winst_dam = opbrengst_ontlaad_dam - kosten_laden_dam
        arb_winst += dag_winst
        arb_winst_dam += dag_winst_dam
        arb_winst_imb += dag_winst - dag_winst_dam

    # -----------------------------------------------------------------------
    # Cat 3: PV-curtail-vermijding (kwartieren met DAM<0 en PV-overschot)
    # Eigen budget: 20% van dagtotaal
    # -----------------------------------------------------------------------
    budget_curtail_per_dag = kwh_budget_per_dag * 0.20
    curtail_budget_resterend = [budget_curtail_per_dag] * n_dagen
    curtail_winst = 0.0

    kandidaten_curtail = []
    for i in range(N):
        if dam_eur_mwh[i] < 0 and pv_kw[i] > consumption_kw[i]:
            overschot_kw = pv_kw[i] - consumption_kw[i]
            kan_laden_kwh = min(overschot_kw, kw_batt) * 0.25
            if kan_laden_kwh < 0.001:
                continue
            waarde = kan_laden_kwh * abs(dam_eur_mwh[i]) / 1000.0
            dag = i // 96
            kandidaten_curtail.append((dag, kan_laden_kwh, waarde))

    kandidaten_curtail.sort(key=lambda x: -(x[2] / x[1] if x[1] > 0 else 0))
    for dag, kwh_nodig, eur_winst in kandidaten_curtail:
        if dag >= n_dagen:
            continue
        b = curtail_budget_resterend[dag]
        if b >= kwh_nodig:
            curtail_winst += eur_winst
            curtail_budget_resterend[dag] -= kwh_nodig
        elif b > 0.001:
            curtail_winst += eur_winst * (b / kwh_nodig)
            curtail_budget_resterend[dag] = 0.0

    # -----------------------------------------------------------------------
    # Cat 1: Maandpiek-shaving
    # Budget: 30% van dag × 30 dagen per maand, alleen ontladen
    # -----------------------------------------------------------------------
    maanden_indices = {}
    for i, ts in enumerate(sim_timestamps):
        m = ts.month
        if m not in maanden_indices:
            maanden_indices[m] = []
        maanden_indices[m].append(i)

    maandpieken_kw = {}
    piekshaving_winst = 0.0

    for m, indices in sorted(maanden_indices.items()):
        if not indices:
            continue
        top10 = sorted(indices, key=lambda i: -netto_afname[i])[:10]
        if not top10:
            continue
        max_piek = netto_afname[top10[0]]
        maandpieken_kw[m] = max_piek
        if max_piek < 1.0:
            continue

        kwh_budget_maand_piek = budget_piek_per_dag * (len(indices) / 96.0)
        max_reductie = min(kw_batt, max_piek)
        drempel_target = max_piek - max_reductie
        kwh_nodig = sum(
            min(max_reductie, max(0.0, netto_afname[idx] - drempel_target)) * 0.25
            for idx in top10
        )
        if kwh_nodig < 0.001:
            continue

        fractie = min(1.0, kwh_budget_maand_piek / kwh_nodig)
        piek_reductie_kw = max_reductie * fractie
        piekshaving_winst += piek_reductie_kw * maandpiek_tarief_per_kw_maand

    # -----------------------------------------------------------------------
    # Schaal naar jaar als sim korter is
    # -----------------------------------------------------------------------
    scale = 365.0 / n_dagen if n_dagen < 365 else 1.0
    arb_winst *= scale
    arb_winst_dam *= scale
    arb_winst_imb *= scale
    curtail_winst *= scale
    piekshaving_winst *= scale

    return {
        'piekshaving_eur': piekshaving_winst,
        'dam_arb_eur': arb_winst_dam,
        'pv_curtail_eur': curtail_winst,
        'imb_arb_eur': arb_winst_imb,
        'maandpieken_kw': maandpieken_kw,
    }



# ---- Hoofdfunctie: analyseer_pieken_en_arbitrage ---------------------------

def analyseer_pieken_en_arbitrage(inp: dict) -> dict:
    """
    Batterij-arbitrage analyse als sales-tool.
    
    Verwacht: zelfde input als run_simulation + optioneel _analyse_config:
      {
        "cycli_per_jaar_default": 540,
        "horizon_jaren": 15,
        "vervanging_toggle": false,
        "capture_rate": 0.018
      }
    
    Returns: JSON conform design-doc batterij-analyse-design.md
      {
        "max_piek_huidig_kw": float,
        "kandidaat_pieken_per_maand": [...],
        "scenarios": [
          { "cycli_per_jaar": 365|540|720|900,
            "levensduur_jaren": float,
            "kwh_dispatch_per_dag": float,
            "per_jaar": [...],
            "cumulatief_horizon_eur": float
          }, ...
        ]
      }
    """
    log.info("=== Batterij-analyse start ===")

    # ---- Configuratie ----
    analyse_conf = inp.get('_analyse_config') or {}
    horizon_jaren = int(analyse_conf.get('horizon_jaren', 15))
    vervanging_toggle = bool(analyse_conf.get('vervanging_toggle', False))
    capture_rate = float(analyse_conf.get('capture_rate', 0.018))

    batt = inp.get('batterij') or {}
    aansluiting = inp.get('aansluiting') or {}
    netbeheer = inp.get('netbeheer') or {}

    kw_batt = float(batt.get('kw', 50))
    kwh_batt = float(batt.get('kwh', 100))
    dod = float(batt.get('dod_pct', 95)) / 100.0 if batt.get('dod_pct', 95) > 1.5 else float(batt.get('dod_pct', 0.95))
    max_cycli = float(batt.get('max_cycli', 8000))
    capex_eur = float(batt.get('capex_eur', 40000))
    kwh_eff = kwh_batt * dod

    # ---- Sim-timestamps ----
    van = parse_iso_date(inp['simulatieperiode']['van'])
    tot = parse_iso_date(inp['simulatieperiode']['tot'])
    sim_timestamps = []
    cur = van
    while cur < tot:
        sim_timestamps.append(cur)
        cur = cur + timedelta(minutes=15)
    N = len(sim_timestamps)
    log.info(f"Sim-periode: {van.date()} → {tot.date()}, {N} kwartieren")

    # ---- Profielen ----
    consumption_kw, pv_kw = _build_full_profiles(inp, sim_timestamps)

    # ---- Marktdata ----
    dam, imb = _get_markt_arrays(inp, N)

    # ---- Huidige max piek ----
    netto_afname = [max(0.0, consumption_kw[i] - pv_kw[i]) for i in range(N)]
    max_piek_kw = max(netto_afname) if netto_afname else 0.0

    # ---- Kandidaat pieken per maand ----
    maanden_data = {}
    for i, ts in enumerate(sim_timestamps):
        m = ts.month
        if m not in maanden_data:
            maanden_data[m] = {'max_kw': 0.0, 'max_ts': ts}
        if netto_afname[i] > maanden_data[m]['max_kw']:
            maanden_data[m]['max_kw'] = netto_afname[i]
            maanden_data[m]['max_ts'] = ts

    kandidaat_pieken = [
        {
            'maand': m,
            'max_piek_kw': round(data['max_kw'], 2),
            'datum': data['max_ts'].isoformat(),
        }
        for m, data in sorted(maanden_data.items())
    ]

    # ---- Scenarios: 365 / 540 / 720 / 900 cycli/jaar ----
    scenario_cycli = [365, 540, 720, 900]
    scenarios = []

    for cycli_per_jaar in scenario_cycli:
        levensduur_jaren = max_cycli / cycli_per_jaar
        kwh_dispatch_per_dag = cycli_per_jaar * kwh_eff / 365.0

        log.info(f"Scenario {cycli_per_jaar} cycli/jaar: levensduur {levensduur_jaren:.1f}j, dispatch {kwh_dispatch_per_dag:.1f} kWh/dag")

        # Jaar-1 revenue via marginale-waarde allocatie
        jaar1 = _alloceer_met_marginale_waarde(
            consumption_kw, pv_kw, dam, imb,
            sim_timestamps, batt, aansluiting, netbeheer,
            cycli_per_jaar, capture_rate
        )
        piekshaving_j1 = jaar1['piekshaving_eur']
        dam_arb_j1 = jaar1['dam_arb_eur']
        pv_curtail_j1 = jaar1['pv_curtail_eur']
        imb_arb_j1 = jaar1['imb_arb_eur']
        totaal_j1 = piekshaving_j1 + dam_arb_j1 + pv_curtail_j1 + imb_arb_j1

        # Per-jaar met lineaire degradatie (M2)
        per_jaar = []
        cumulatief = 0.0
        vervanging_jaar = levensduur_jaren
        vorige_vervanging = 0

        for j in range(1, horizon_jaren + 1):
            # Bepaal huidige batterij-leeftijd (na vervanging)
            leeftijd = j - vorige_vervanging
            huidige_levensduur = max_cycli / cycli_per_jaar

            if leeftijd <= huidige_levensduur:
                # Lineaire degradatie: 100% → 80% over volledige levensduur
                cap_pct = 1.0 - (leeftijd / huidige_levensduur) * 0.20
                cap_pct = max(0.80, min(1.0, cap_pct))
                actief = True
                vervanging_capex = 0.0
            elif vervanging_toggle:
                # Vervanging: nieuwe batterij
                vorige_vervanging = j - 1
                leeftijd = 1
                cap_pct = 1.0 - (leeftijd / (max_cycli / cycli_per_jaar)) * 0.20
                actief = True
                vervanging_capex = -capex_eur  # negatief = uitgave
            else:
                cap_pct = 0.0
                actief = False
                vervanging_capex = 0.0

            jaar_piekshaving = round(piekshaving_j1 * cap_pct, 2) if actief else 0.0
            jaar_dam = round(dam_arb_j1 * cap_pct, 2) if actief else 0.0
            jaar_pv_curtail = round(pv_curtail_j1 * cap_pct, 2) if actief else 0.0
            jaar_imb = round(imb_arb_j1 * cap_pct, 2) if actief else 0.0
            jaar_totaal = round(jaar_piekshaving + jaar_dam + jaar_pv_curtail + jaar_imb + vervanging_capex, 2)

            cumulatief += jaar_totaal

            per_jaar.append({
                'jaar': j,
                'piekshaving_eur': jaar_piekshaving,
                'dam_arb_eur': jaar_dam,
                'pv_curtail_eur': jaar_pv_curtail,
                'imb_arb_eur': jaar_imb,
                'totaal_eur': jaar_totaal,
                'batterij_capaciteit_pct': round(cap_pct * 100, 1),
                'actief': actief,
                'vervanging_capex_eur': vervanging_capex,
            })

        scenarios.append({
            'cycli_per_jaar': cycli_per_jaar,
            'levensduur_jaren': round(levensduur_jaren, 1),
            'kwh_dispatch_per_dag': round(kwh_dispatch_per_dag, 2),
            'per_jaar': per_jaar,
            'cumulatief_horizon_eur': round(cumulatief, 2),
            '_jaar1_detail': {
                'piekshaving_eur': round(piekshaving_j1, 2),
                'dam_arb_eur': round(dam_arb_j1, 2),
                'pv_curtail_eur': round(pv_curtail_j1, 2),
                'imb_arb_eur': round(imb_arb_j1, 2),
                'totaal_eur': round(totaal_j1, 2),
            },
        })

    log.info(f"Analyse klaar: {len(scenarios)} scenarios")

    return {
        'max_piek_huidig_kw': round(max_piek_kw, 2),
        'kandidaat_pieken_per_maand': kandidaat_pieken,
        'scenarios': scenarios,
        '_versie': 'v1.5-M1M2M3',
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    try:
        inp = json.load(sys.stdin)
    except Exception as e:
        log.error(f"Kon input-JSON niet lezen: {e}")
        sys.exit(1)

    # Kies modus: analyse of simulatie
    modus = inp.get('_modus', 'simulatie')

    try:
        if modus == 'analyse':
            out = analyseer_pieken_en_arbitrage(inp)
        else:
            out = run_simulation(inp)
    except Exception as e:
        log.error(f"Verwerking gefaald: {e}", exc_info=True)
        sys.exit(2)

    json.dump(out, sys.stdout, indent=2, default=str)


if __name__ == '__main__':
    main()
