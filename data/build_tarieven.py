#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_tarieven.py — genereert tarieven.json uit de OFFICIELE tarieflijsten.
Fluctus.net CVSO — v1.0 (juli 2026)

WAAROM DIT BESTAAT
==================
tarieven.json werd ooit met de hand samengesteld en bevatte drie fouten die pas
opvielen toen een klantcase een onverklaarbaar LS/MS-verschil gaf:

  1. DUBBELTELLING van de Elia-transmissienetkosten in Vlaanderen en Brussel.
     De VREG-tarieflijst zegt letterlijk: "Deze tarieflijst omvat de
     distributienettarieven INCLUSIEF de transmissienetkosten." Wij telden er
     nog eens een apart transport_*-blok bovenop: op LS +278,91 EUR/kW/jaar
     bovenop een tarief van 57,10 (5,9x), op MS +60,17 bovenop 101,41 (1,6x).
     Die asymmetrie blies het LS/MS-verschil op met ~32.000 EUR/jaar — meer dan
     het hele werkelijke verschil.
  2. LS-posten odv/surcharges/reactief stonden op 0. Voor West moet dat
     33,2887 / 1,8227 / 13,4149 zijn.
  3. Het LS-transportblok was een kolomverschuiving van MS (bij alle 13 DNB's
     schoven dezelfde zes waarden een positie op).

Fout 2 en 3 maskeerden elkaar deels: de verschoven marktintegratie (27,52
EUR/MWh) stond toevallig ongeveer waar de ontbrekende ODV (25-33 EUR/MWh)
hoorde. Op de EUR/MWh-as viel het grotendeels weg tegen elkaar, op de EUR/kW-as
niet. Precies daarom is handwerk hier gevaarlijk en genereren we voortaan.

REGIONAAL VERSCHIL — DE KERN VAN HET TRANSPORT-VERHAAL
======================================================
  Vlaanderen (VREG) : transmissiekosten ZITTEN AL in het DNB-tarief -> transport_* = 0
  Brussel (BRUGEL)  : transport is een aparte REGEL in dezelfde lijst
                      (Sibelga 2026: 0,0214403 EUR/kWh) -> als volumetrische post,
                      NIET als apart kW-blok
  Wallonie (CWaPE)  : transmissiekosten zitten NIET in het DNB-tarief. Er is een
                      aparte, GEPEREQUATEERDE transportlijst die voor alle Waalse
                      GRD's identiek is -> transport_* WEL invullen

Wie dit onderscheid negeert en een van beide modellen op heel Belgie toepast,
krijgt exact de fout die we net rechtgezet hebben.

JAARLIJKSE HERHALING (dit is het hele punt van dit script)
=========================================================
De VREG publiceert de tarieflijsten voor jaar N rond november van jaar N-1.

  1. Download de 8 PDF's naar ./tariefkaarten/<jaar>/
       https://assets.vlaamsenutsregulator.be/<jjjj-mm>/<CODE>%20-%20<jaar>%20-%20ELEK.pdf
     met CODE = FA FHV FI FK FL FMV FW FZD
     (de map <jjjj-mm> is de publicatiemaand, bv. 2025-11 voor tariefjaar 2026)
  2. Werk tarieven/wallonie_brussel_<jaar>.json bij (handmatig — zie hieronder)
  3. python3 build_tarieven.py --pdf-dir ./tariefkaarten/<jaar> --jaar <jaar>
  4. Lees tarieven_rapport.txt. Bij ELKE waarschuwing: niet deployen voor je ze snapt.
  5. Toets af aan een echte klantfactuur (validate_factuur.py)

WAAROM WALLONIE/BRUSSEL HANDMATIG BLIJFT
========================================
Hun tariefstructuur past niet op ons schema en is niet veilig automatisch te
mappen:
  - Wallonie kent GEEN "toegangsvermogen" (EUR/kVA/jaar). Capaciteit wordt op de
    GEMETEN piek gefactureerd. Onze mapping is een bewuste vertaling:
        pointe annuelle  (EUR/kW/maand) x 12 -> toegangsvermogen_eur_kw_jaar
        pointe mensuelle (EUR/kW/maand) x 12 -> maandpiek_eur_kw_jaar
    Geverifieerd: CWaPE ORES 2026 pointe mensuelle 2,7943885 x 12 = 33,532662,
    exact onze ORES|MS maandpiek. Idem pointe annuelle 1,3971943 x 12 = 16,7663316.
  - Wallonie differentieert proportioneel naar HP/HC; wij hebben een vlak tarief.
  - Brussel gebruikt EUR/kW/dag en een degressiviteitscoefficient.
Een parser die dit "gokt" is erger dan een mens die het bewust invult. Vandaar:
handmatig, mét bronvermelding en datum, en dit script controleert de vorm.
"""

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

VERSIE = "1.0"

# ---------------------------------------------------------------------------
# Vlaamse zones: VREG-bestandscode -> zonenaam zoals de simulator ze kent.
# De zonenaam MOET matchen met data/profielen en de postcode-mapping.
# ---------------------------------------------------------------------------
VREG_ZONES = {
    "FA":  "Antwerpen",
    "FHV": "Halle-Vilv.",
    "FI":  "Imewo",
    "FK":  "Kempen",
    "FL":  "Limburg",
    "FMV": "Midden-Vl.",
    "FW":  "West",
    "FZD": "Zenne-Dijle",
}

# ---------------------------------------------------------------------------
# Kolomindeling van de VREG-tarieflijst. Dit is de enige plek waar de mapping
# staat; de rest van het script leidt er alles uit af.
#
#   rijen met 5 waarden: [26-36 kV-post, 26-36 kV-net, 1-26 kV-post,
#                         1-26 kV-net, distributiecabine]
#   rijen met 8 waarden: idem + [LS-piekmeting, LS-analoge meter, prosumenten]
#   Maximumtarief (6)  : idem 5 + [LS-piekmeting]
#
# Wij bedienen exact twee klantengroepen:
#   MS = "1-26 kV-net"                        -> index 3
#   LS = "laagspanningsnet met piekmeting"    -> index 5 (en sectie 1.2)
# ---------------------------------------------------------------------------
IDX_MS = 3
IDX_LS = 5

# Controlewaarden. Handmatig geverifieerd tegen de PDF's van 2026. Als de
# VREG-lay-out ooit wijzigt, faalt het script HIEROP en niet stilletjes op de
# cijfers. Dat is met opzet: liever een harde stop dan een verkeerde factuur.
CONTROLE_2026 = {
    ("Antwerpen", "MS", "toegangsvermogen_eur_kw_jaar"): 33.6582672,
    ("Antwerpen", "MS", "maandpiek_eur_kw_jaar"):        54.3035736,
    ("Antwerpen", "MS", "odv_eur_mwh"):                   3.8921,
    ("Antwerpen", "LS", "maandpiek_eur_kw_jaar"):        49.4036563,
    ("Antwerpen", "LS", "proportioneel_eur_mwh"):        23.4492,
    ("West",      "MS", "toegangsvermogen_eur_kw_jaar"): 41.6436000,
    ("West",      "MS", "maandpiek_eur_kw_jaar"):        59.7614052,
    ("West",      "LS", "maandpiek_eur_kw_jaar"):        57.0995726,
    ("West",      "LS", "proportioneel_eur_mwh"):        28.0823,
}

# Velden die elke record moet hebben. Ontbreekt er een -> harde fout.
VERPLICHT = [
    "maandpiek_eur_kw_jaar", "toegangsvermogen_eur_kw_jaar",
    "overschrijding_toegangsvermogen_eur_kw_jaar", "proportioneel_eur_mwh",
    "databeheer_eur_jaar", "reactief_eur_mvarh", "odv_eur_mwh",
    "surcharges_eur_mwh", "soldes_eur_mwh", "maximumtarief_eur_mwh",
    "transport_maandpiek_eur_kw_mnd", "transport_jaarpiek_eur_kw_jaar",
    "transport_systeembeheer_eur_mwh", "transport_reserves_eur_mwh",
    "transport_marktintegratie_eur_mwh", "transport_beschikbaar_eur_kva_jaar",
    "transport_reactief_eur_mvarh", "energiefonds_eur_jaar",
    "accijns_basis_eur_mwh", "accijns_schijf1_3mwh", "accijns_schijf2_20mwh",
    "accijns_schijf3_50mwh", "accijns_schijf4_1000mwh", "accijns_schijf5_inf",
]

# Vlaanderen-brede posten die NIET in de VREG-tarieflijst staan (ze komen van de
# federale/Vlaamse overheid, niet van de netbeheerder). Jaarlijks te herzien.
VLAAMSE_HEFFINGEN_2026 = {
    "energiefonds_eur_jaar_ms": 2305.32,   # Vlaamse energieheffing, MS-schijf
    "energiefonds_eur_jaar_ls": 120.84,    # idem, LS-schijf
    "accijns_basis_eur_mwh_ls": 1.9261,
    "accijns_basis_eur_mwh_ms": 0.0,
    "accijns_schijf1_3mwh": 14.21,
    "accijns_schijf2_20mwh": 14.21,
    "accijns_schijf3_50mwh": 12.09,
    "accijns_schijf4_1000mwh": 11.39,
    "accijns_schijf5_inf": 10.00,
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _getallen(regel: str):
    """Alle Belgische decimale getallen op een regel, in volgorde.

    LET OP — dit was een echte bug in v0.9: labels bevatten zelf getallen.
    De rij '26-36 kV, 1-26 kV, distributiecabine EUR/jaar 57,65 57,65 ...' gaf
    databeheer = 26.0 in plaats van 57,65, omdat de '26' uit '26-36 kV' als
    eerste getal werd opgepikt. Idem voor '1-26 kV'.

    Oplossing: knip af op de EENHEIDSMARKERING. In elke VREG-rij staan de
    waarden altijd NA een 'EUR/...'-token ('EUR/jaar', 'EUR/kWh', 'EUR/kVA/jaar',
    'EUR/kVArh', ...). Alles ervoor is label en mag nooit meetellen. We nemen de
    LAATSTE EUR/-token op de regel, want rijen als 'Toegangsvermogen
    EUR/kVA/maand ...' hebben er maar een, en '*3EUR/kWh' plakt tegen het label.
    """
    m = None
    for m in re.finditer(r"EUR\s*/\s*\S+", regel):
        pass
    staart = regel[m.end():] if m else regel
    uit = []
    for g in re.finditer(r"(?<![\d,.\-])(\d{1,4}(?:,\d+)?)(?![\d,]*\s*%)", staart):
        try:
            uit.append(float(g.group(1).replace(",", ".")))
        except ValueError:
            pass
    return uit


def _regel_na(tekst: str, label: str, offset: int = 0):
    """Zoek de regel die met `label` begint en geef de regel `offset` verder.

    Tolerant voor het sectienummer dat VREG voor het label zet: de toeslagenrij
    heet '5 Tarieven voor de toeslagen *3EUR/kWh ...', niet 'Tarieven voor de
    toeslagen'. Een strikte startswith() gaf daar stil None (v0.9-bug).
    """
    regels = tekst.split("\n")
    for i, r in enumerate(regels):
        kaal = re.sub(r"^\s*\d+(?:\.\d+)?\s+", "", r.strip())
        if kaal.startswith(label) or r.strip().startswith(label):
            j = i + offset
            if 0 <= j < len(regels):
                return regels[j]
    return None


def parse_vreg_tekst(tekst: str, zone: str, waarschuw: list) -> dict:
    """Parse de tekst van een VREG-tarieflijst naar twee records (MS en LS).

    Label-verankerd, niet positie-verankerd: als VREG een rij toevoegt of de
    volgorde wijzigt, blijft dit werken. Wijzigt een LABEL, dan faalt het hard
    op de controlewaarden — precies de bedoeling.
    """
    def pak(label, idx, offset=0, deel_door=1.0, maal=1.0, verplicht=True):
        regel = _regel_na(tekst, label, offset)
        if regel is None:
            if verplicht:
                waarschuw.append(f"{zone}: label niet gevonden -> {label!r}")
            return None
        g = _getallen(regel)
        if len(g) <= idx:
            if verplicht:
                waarschuw.append(
                    f"{zone}: rij {label!r} heeft {len(g)} waarden, index {idx} gevraagd"
                )
            return None
        return round(g[idx] * maal / deel_door, 7)

    ms, ls = {}, {}

    # --- 1 Tarieven voor het netgebruik ---------------------------------------
    # 'of EUR/kVA/jaar' staat één regel onder 'Toegangsvermogen'.
    ms["toegangsvermogen_eur_kw_jaar"] = pak("Toegangsvermogen", IDX_MS, offset=1)
    ms["maandpiek_eur_kw_jaar"] = pak("Maandpiek", IDX_MS, offset=1)
    ms["overschrijding_toegangsvermogen_eur_kw_jaar"] = pak(
        "Tarief voor overschrijding toegangsvermogen", IDX_MS, offset=1)
    # MS heeft geen volumetrisch netgebruikstarief — dat is de kern van het
    # LS/MS-verschil (LS ~23-28 EUR/MWh, MS 0).
    ms["proportioneel_eur_mwh"] = 0.0

    # LS = sectie 1.2 'Afnameklanten op laagspanningsnet met piekmeting'
    ls["maandpiek_eur_kw_jaar"] = pak("Gemiddelde maandpiek", 0)
    ls["proportioneel_eur_mwh"] = pak("kWh-tarief EUR/kWh", 0, maal=1000.0)
    ls["toegangsvermogen_eur_kw_jaar"] = 0.0
    ls["overschrijding_toegangsvermogen_eur_kw_jaar"] = 0.0

    # --- 2 Reactief -----------------------------------------------------------
    reac = pak("Tarief voor overschrijding forfaitair toegelaten hoeveelheid",
               0, maal=1000.0)
    ms["reactief_eur_mvarh"] = reac
    ls["reactief_eur_mvarh"] = reac

    # --- 3 Databeheer ---------------------------------------------------------
    ms["databeheer_eur_jaar"] = pak("26-36 kV, 1-26 kV, distributiecabine", 0)
    ls["databeheer_eur_jaar"] = pak("Laagspanningnet EUR/jaar", 0)

    # --- 4 Openbaredienstverplichtingen --------------------------------------
    ms["odv_eur_mwh"] = pak("kWh-tarief normaal", IDX_MS, maal=1000.0)
    ls["odv_eur_mwh"] = pak("kWh-tarief normaal", IDX_LS, maal=1000.0)

    # --- 5 Toeslagen ----------------------------------------------------------
    ms["surcharges_eur_mwh"] = pak("Tarieven voor de toeslagen", IDX_MS, maal=1000.0)
    ls["surcharges_eur_mwh"] = pak("Tarieven voor de toeslagen", IDX_LS, maal=1000.0)

    # --- Maximumtarief --------------------------------------------------------
    ms["maximumtarief_eur_mwh"] = pak("Maximumtarief", IDX_MS, maal=1000.0)
    ls["maximumtarief_eur_mwh"] = pak("Maximumtarief", IDX_LS, maal=1000.0)

    # --- Posten die niet in deze lijst staan ---------------------------------
    for rec in (ms, ls):
        rec["soldes_eur_mwh"] = 0.0        # Vlaanderen kent geen soldes-post
        # HIER ZIT DE FIX: VREG-tarieven bevatten de transmissiekosten al.
        # Elke waarde != 0 in dit blok is een dubbeltelling.
        rec["transport_maandpiek_eur_kw_mnd"] = 0.0
        rec["transport_jaarpiek_eur_kw_jaar"] = 0.0
        rec["transport_systeembeheer_eur_mwh"] = 0.0
        rec["transport_reserves_eur_mwh"] = 0.0
        rec["transport_marktintegratie_eur_mwh"] = 0.0
        rec["transport_beschikbaar_eur_kva_jaar"] = 0.0
        rec["transport_reactief_eur_mvarh"] = 0.0
        rec["accijns_schijf1_3mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_schijf1_3mwh"]
        rec["accijns_schijf2_20mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_schijf2_20mwh"]
        rec["accijns_schijf3_50mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_schijf3_50mwh"]
        rec["accijns_schijf4_1000mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_schijf4_1000mwh"]
        rec["accijns_schijf5_inf"] = VLAAMSE_HEFFINGEN_2026["accijns_schijf5_inf"]

    ms["energiefonds_eur_jaar"] = VLAAMSE_HEFFINGEN_2026["energiefonds_eur_jaar_ms"]
    ls["energiefonds_eur_jaar"] = VLAAMSE_HEFFINGEN_2026["energiefonds_eur_jaar_ls"]
    ms["accijns_basis_eur_mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_basis_eur_mwh_ms"]
    ls["accijns_basis_eur_mwh"] = VLAAMSE_HEFFINGEN_2026["accijns_basis_eur_mwh_ls"]

    return {"MS": ms, "LS": ls}


def lees_bron(pad: Path) -> str:
    """PDF (via pdfplumber) of reeds geextraheerde .txt."""
    if pad.suffix.lower() == ".txt":
        return pad.read_text(encoding="utf-8")
    try:
        import pdfplumber
    except ImportError:
        sys.exit("pdfplumber ontbreekt. Installeer: pip install pdfplumber")
    with pdfplumber.open(str(pad)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


# ---------------------------------------------------------------------------
# Validatie
# ---------------------------------------------------------------------------

def valideer(tarieven: dict, jaar: int, waarschuw: list, fouten: list):
    # 1. Controlewaarden (alleen zinvol voor tariefjaar 2026)
    if jaar == 2026:
        for (zone, sp, veld), verwacht in CONTROLE_2026.items():
            key = f"{zone}|{sp}"
            if key not in tarieven:
                fouten.append(f"controle: {key} ontbreekt volledig")
                continue
            echt = tarieven[key].get(veld)
            if echt is None or abs(echt - verwacht) > 0.0001:
                fouten.append(
                    f"CONTROLE FAALT {key}.{veld}: {echt} != {verwacht} "
                    f"-> kolom-mapping of PDF-lay-out gewijzigd, NIET deployen"
                )

    for key, rec in tarieven.items():
        if key.startswith("_"):
            continue
        zone, sp = key.split("|")

        # 2. Volledigheid
        for veld in VERPLICHT:
            if veld not in rec or rec[veld] is None:
                fouten.append(f"{key}: verplicht veld ontbreekt of is null -> {veld}")

        # 3. Plausibiliteit — vangt de kolomverschuiving die we net rechtzetten
        mp = rec.get("maandpiek_eur_kw_jaar") or 0
        if not (10 <= mp <= 200):
            waarschuw.append(f"{key}: maandpiek {mp} EUR/kW/jaar buiten 10-200 — nakijken")
        # NB: "transport > DNB-tarief" is GEEN bruikbare check. In Wallonie is het
        # transporttarief geperequateerd (identiek voor alle GRD's), dus bij een
        # kleine DNB als AIEG (maandpiek 29,00) is transport (45,73) legitiem groter.
        # De echte invariant per regio staat hieronder in _check_transport().

        # 4. Regio-regel voor transport (zie _check_transport voor de perequatie)
        regio = rec.get("_regio")
        tr_som = sum(abs(rec.get(v) or 0) for v in (
            "transport_maandpiek_eur_kw_mnd", "transport_jaarpiek_eur_kw_jaar",
            "transport_systeembeheer_eur_mwh", "transport_reserves_eur_mwh",
            "transport_marktintegratie_eur_mwh", "transport_beschikbaar_eur_kva_jaar"))
        if regio in ("Vlaanderen", "Brussel") and tr_som > 0:
            fouten.append(
                f"{key}: regio {regio} heeft transport_* != 0 -> DUBBELTELLING. "
                f"De DNB-tarieflijst bevat de transmissiekosten daar al."
            )
        if regio == "Wallonie" and tr_som == 0:
            waarschuw.append(
                f"{key}: regio Wallonie heeft transport_* = 0 -> ONDERTELLING. "
                f"Waalse DNB-tarieven bevatten Elia NIET; transport moet apart."
            )

        # 5. LS/MS-structuur
        if sp == "MS" and (rec.get("proportioneel_eur_mwh") or 0) > 0 and regio == "Vlaanderen":
            waarschuw.append(f"{key}: MS heeft in Vlaanderen normaal geen volumetrisch netgebruikstarief")
        if sp == "LS" and (rec.get("toegangsvermogen_eur_kw_jaar") or 0) > 0 and regio == "Vlaanderen":
            waarschuw.append(f"{key}: LS heeft in Vlaanderen normaal geen toegangsvermogen")
        if sp == "LS" and (rec.get("odv_eur_mwh") or 0) == 0:
            waarschuw.append(f"{key}: odv = 0 op LS — dat was fout #2 in v0, nakijken")

    _check_transport(tarieven, waarschuw, fouten)


def _check_transport(tarieven, waarschuw, fouten):
    """Waalse perequatie: Elia-transport is sinds 01/03/2019 identiek voor ALLE
    Waalse GRD's ("Depuis le 1er mars 2019, ils sont perequates. Cela signifie que
    les memes tarifs sont appliques aux utilisateurs de reseau partout en Wallonie,
    peu importe leur gestionnaire de reseau." — CWaPE).

    Dat is een harde, controleerbare invariant: wijkt een Waalse zone af, dan is er
    per zone geknoeid of is er een waarde uit het verkeerde jaar blijven staan.
    Dit vervangt de naieve "transport > DNB"-check, die op AIEG en ORES vals alarm
    gaf omdat hun distributietarief nu eenmaal laag is.
    """
    velden = ("transport_maandpiek_eur_kw_mnd", "transport_jaarpiek_eur_kw_jaar",
              "transport_systeembeheer_eur_mwh", "transport_reserves_eur_mwh",
              "transport_marktintegratie_eur_mwh", "transport_beschikbaar_eur_kva_jaar")
    waals = {k: r for k, r in tarieven.items()
             if not k.startswith("_") and r.get("_regio") == "Wallonie"}
    if len(waals) < 2:
        return
    ref_key = sorted(waals)[0]
    ref = waals[ref_key]
    for key, rec in sorted(waals.items()):
        for v in velden:
            a, b = rec.get(v) or 0, ref.get(v) or 0
            if abs(a - b) > 1e-6:
                fouten.append(
                    f"{key}.{v} = {a} wijkt af van {ref_key} ({b}). Waals transport is "
                    f"GEPEREQUATEERD en moet identiek zijn voor alle GRD's."
                )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Genereer tarieven.json uit de officiele VREG/CWaPE/BRUGEL-tarieflijsten.")
    ap.add_argument("--pdf-dir", type=Path, default=None,
                    help="Map met de 8 VREG-PDF's (FA/FHV/FI/FK/FL/FMV/FW/FZD), of .txt-extracties")
    ap.add_argument("--vlaanderen-json", type=Path, default=None,
                    help="Bootstrap: reeds geverifieerde Vlaamse waarden i.p.v. de PDF's opnieuw "
                         "parsen. Zo is tarieven.json 2026 gemaakt (de PDF's konden niet lokaal "
                         "gedownload worden). Bij een JAARLIJKSE update gebruik je --pdf-dir.")
    ap.add_argument("--jaar", required=True, type=int, help="Tariefjaar, bv. 2026")
    ap.add_argument("--wb", type=Path, default=None,
                    help="wallonie_brussel_<jaar>.json (default: naast dit script)")
    ap.add_argument("--out", type=Path, default=Path("tarieven.json"))
    ap.add_argument("--rapport", type=Path, default=Path("tarieven_rapport.txt"))
    ap.add_argument("--force", action="store_true",
                    help="Schrijf ook weg bij FOUTEN (alleen voor debug)")
    args = ap.parse_args()

    waarschuw, fouten = [], []
    tarieven = {}
    bronnen = {}

    if not args.pdf_dir and not args.vlaanderen_json:
        sys.exit("Geef --pdf-dir (normale jaarlijkse update) of --vlaanderen-json (bootstrap).")

    # --- Vlaanderen: bootstrap uit geverifieerde JSON -------------------------
    if args.vlaanderen_json:
        vl = json.loads(args.vlaanderen_json.read_text(encoding="utf-8"))
        for key, rec in vl.items():
            if key.startswith("_"):
                continue
            zone, sp = key.split("|")
            rec["_regio"] = "Vlaanderen"
            rec["_tariefjaar"] = args.jaar
            # Dezelfde verrijking als de PDF-weg, zodat beide paden identiek zijn.
            rec["soldes_eur_mwh"] = 0.0
            for v in ("transport_maandpiek_eur_kw_mnd", "transport_jaarpiek_eur_kw_jaar",
                      "transport_systeembeheer_eur_mwh", "transport_reserves_eur_mwh",
                      "transport_marktintegratie_eur_mwh", "transport_beschikbaar_eur_kva_jaar",
                      "transport_reactief_eur_mvarh"):
                rec[v] = 0.0
            for k2 in ("accijns_schijf1_3mwh", "accijns_schijf2_20mwh", "accijns_schijf3_50mwh",
                       "accijns_schijf4_1000mwh", "accijns_schijf5_inf"):
                rec[k2] = VLAAMSE_HEFFINGEN_2026[k2]
            rec["energiefonds_eur_jaar"] = VLAAMSE_HEFFINGEN_2026[
                "energiefonds_eur_jaar_ms" if sp == "MS" else "energiefonds_eur_jaar_ls"]
            rec["accijns_basis_eur_mwh"] = VLAAMSE_HEFFINGEN_2026[
                "accijns_basis_eur_mwh_ms" if sp == "MS" else "accijns_basis_eur_mwh_ls"]
            tarieven[key] = rec
        bronnen["_vlaanderen"] = args.vlaanderen_json.name

    # --- Vlaanderen: automatisch uit de VREG-PDF's ---------------------------
    for code, zone in (VREG_ZONES.items() if args.pdf_dir else []):
        kand = [p for p in args.pdf_dir.iterdir()
                if p.is_file() and re.match(rf"^{code}\s*-\s*{args.jaar}\s*-\s*ELEK\.(pdf|txt)$",
                                            p.name, re.I)]
        if not kand:
            fouten.append(f"{zone}: geen bestand '{code} - {args.jaar} - ELEK.pdf' in {args.pdf_dir}")
            continue
        tekst = lees_bron(kand[0])
        recs = parse_vreg_tekst(tekst, zone, waarschuw)
        for sp, rec in recs.items():
            rec["_regio"] = "Vlaanderen"
            rec["_bron"] = (f"https://assets.vlaamsenutsregulator.be/.../"
                            f"{code}%20-%20{args.jaar}%20-%20ELEK.pdf")
            rec["_tariefjaar"] = args.jaar
            tarieven[f"{zone}|{sp}"] = rec
        bronnen[zone] = kand[0].name

    # --- Wallonie + Brussel: handmatig onderhouden ---------------------------
    wb_pad = args.wb or (Path(__file__).parent / f"wallonie_brussel_{args.jaar}.json")
    if not wb_pad.exists():
        fouten.append(f"Wallonie/Brussel-bestand ontbreekt: {wb_pad}")
    else:
        wb = json.loads(wb_pad.read_text(encoding="utf-8"))
        for key, rec in wb.items():
            if key.startswith("_"):
                continue
            rec.setdefault("_tariefjaar", args.jaar)
            tarieven[key] = rec
        bronnen["_wallonie_brussel"] = wb_pad.name

    valideer(tarieven, args.jaar, waarschuw, fouten)

    tarieven["_meta"] = {
        "gegenereerd_door": f"build_tarieven.py v{VERSIE}",
        "gegenereerd_op": date.today().isoformat(),
        "tariefjaar": args.jaar,
        "bronbestanden": bronnen,
        "zones": len([k for k in tarieven if not k.startswith("_")]),
        "transport_regel": {
            "Vlaanderen": "transport_* = 0 — VREG: tarieflijst is INCLUSIEF transmissienetkosten",
            "Brussel": "transport_* = 0 — zit als aparte regel in de Sibelga-grille (in odv/proportioneel verwerkt)",
            "Wallonie": "transport_* ingevuld — CWaPE: Elia wordt apart geperequateerd doorgerekend",
        },
    }

    # --- Rapport -------------------------------------------------------------
    lijnen = [
        f"tarieven.json — bouwrapport",
        f"gegenereerd : {date.today().isoformat()} door build_tarieven.py v{VERSIE}",
        f"tariefjaar  : {args.jaar}",
        f"zones       : {len([k for k in tarieven if not k.startswith('_')])}",
        "",
    ]
    if fouten:
        lijnen += ["FOUTEN (blokkerend):"] + [f"  x {f}" for f in fouten] + [""]
    if waarschuw:
        lijnen += ["WAARSCHUWINGEN (nakijken):"] + [f"  ! {w}" for w in waarschuw] + [""]
    if not fouten and not waarschuw:
        lijnen += ["Geen fouten, geen waarschuwingen.", ""]

    lijnen += ["Overzicht LS vs MS (EUR/MWh volumetrisch, EUR/kW/jaar capaciteit):", ""]
    lijnen += [f"  {'zone':14s} {'sp':3s} {'volum.':>9s} {'maandpiek':>10s} {'toegang':>9s}"]
    for key in sorted(k for k in tarieven if not k.startswith("_")):
        r = tarieven[key]
        zone, sp = key.split("|")
        vol = sum((r.get(v) or 0) for v in ("proportioneel_eur_mwh", "odv_eur_mwh",
                                            "surcharges_eur_mwh", "soldes_eur_mwh"))
        lijnen.append(f"  {zone:14s} {sp:3s} {vol:9.2f} {r.get('maandpiek_eur_kw_jaar') or 0:10.2f} "
                      f"{r.get('toegangsvermogen_eur_kw_jaar') or 0:9.2f}")

    args.rapport.write_text("\n".join(lijnen) + "\n", encoding="utf-8")

    if fouten and not args.force:
        print("\n".join(lijnen))
        sys.exit(f"\nGESTOPT: {len(fouten)} blokkerende fout(en). "
                 f"tarieven.json NIET geschreven. Zie {args.rapport}.")

    args.out.write_text(json.dumps(tarieven, indent=1, ensure_ascii=False) + "\n",
                        encoding="utf-8")
    print("\n".join(lijnen))
    print(f"\n-> {args.out} geschreven ({len([k for k in tarieven if not k.startswith('_')])} records)")
    if waarschuw:
        print(f"-> {len(waarschuw)} waarschuwing(en); lees {args.rapport} voor je deployt.")


if __name__ == "__main__":
    main()
