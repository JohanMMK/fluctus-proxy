#!/usr/bin/env python3
# =============================================================================
#  FLUCTUS — Financieel-rapport GENERATOR  (OFFLINE / CLI-TOOL)
#  Neemt de simulator-JSON's van twee scenario's (beter + minder) + een config
#  en schrijft een kant-en-klaar rapport-HTML (financieel-rapport.html als
#  sjabloon), gevoed via window.FLUCTUS_RAPPORT_DATA.
#
#  ── DRAAIT NIET OP RAILWAY / DE PORTAL ──────────────────────────────────────
#  Dit is een handmatig hulpprogramma. De portal-knop "Genereer financieel
#  rapport" bouwt het rapport CLIENT-SIDE op (uit de sim-resultaten in de
#  browser, via sessionStorage) en gebruikt dit script NIET. Deze generator is
#  het OFFLINE pad: voor batch/handmatige (her)generatie met een eigen config,
#  of om een rapport buiten de simulator om samen te stellen.
#  Het maakt GEEN factuurrapport.html (dat wordt client-side door simulator.html
#  gevoed) — enkel het grote financieel rapport.
#
#  Bewaren in git: aanbevolen in de fluctus-proxy repo onder tools/ , naast
#  simulator.py (dezelfde sim-JSON's als invoer). In git zetten = versioneren en
#  bijderhand houden; het wordt daardoor NIET automatisch uitgevoerd.
#
#  Gebruik:
#    python3 rapport_generator.py config.json  [sjabloon.html]  [uit.html]
#
#  De config verwijst naar de sim-JSON-bestanden en levert de gegevens die NIET
#  uit de sim komen (bestaande factuur, scherp dynamisch contract, ontwerp-specs).
#  Zie voorbeeld_config() onderaan voor het exacte formaat.
# =============================================================================
import json, sys, os


def _g(d, *path, default=0.0):
    """Veilige geneste .get()."""
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur


def _factuur_componenten(sim):
    """Haal de factuurcomponenten (energie / distributie / capaciteit / heffingen /
    subtotaal) uit een sim-JSON."""
    jf = sim.get('jaarfactuur', {})
    gr = jf.get('groepen', {})
    A = _g(gr, 'A_energiekost', '_subtotaal')
    B = _g(gr, 'B_netgebruik_afname', '_subtotaal')
    C = _g(gr, 'C_netgebruik_injectie', '_subtotaal')
    D = _g(gr, 'D_transport', '_subtotaal')
    E = _g(gr, 'E_heffingen', '_subtotaal')
    Bd = gr.get('B_netgebruik_afname', {}) if isinstance(gr.get('B_netgebruik_afname'), dict) else {}
    Dd = gr.get('D_transport', {}) if isinstance(gr.get('D_transport'), dict) else {}
    cap = (Bd.get('toegangsvermogen', 0) + Bd.get('maandpiek', 0) + Bd.get('overschrijding_toegangsvermogen', 0)
           + Dd.get('beschikbaar_vermogen', 0) + Dd.get('maandpiek_transport', 0) + Dd.get('jaarpiek_transport', 0))
    return {
        'energie': round(A),
        'distributie': round(B + C + D),
        'capaciteit': round(cap),
        'heffingen': round(E),
        'subtotaal': round(jf.get('subtotaal_excl_btw', 0)),
    }


def _energie(sim):
    """Energetische kerncijfers uit een sim-JSON (van het BETERE scenario)."""
    kpi = sim.get('kpi', {})
    lp = sim.get('laadplein', {})
    jf = sim.get('jaarfactuur', {})
    return {
        'pv_productie_mwh': round(kpi.get('totaal_pv_mwh', 0), 1),
        'pv_zelfverbruik_mwh': round(kpi.get('pv_naar_eigen_verbruik_mwh', kpi.get('totaal_pv_mwh', 0) * kpi.get('pct_zelfconsumptie', 0) / 100.0), 1),
        'pv_via_batterij_mwh': round(kpi.get('pv_via_batterij_mwh', 0), 1),
        'pv_naar_gebouw_mwh': round(kpi.get('pv_naar_gebouw_mwh', 0), 1),
        'pv_naar_laadplein_mwh': round(kpi.get('pv_naar_laadplein_mwh', 0), 1),
        'batterij_doorzet_mwh': round(kpi.get('energie_ontladen_mwh', 0), 1),
        'laadplein_mwh': round(lp.get('ev_last_mwh', 0), 1),
        'aansluiting_kw': round(jf.get('toegangsvermogen_kw', 0), 0),
        'sitepiek_zonder_batterij_kw': round(_g(lp, 'capaciteit', 'benodigd_toegangsvermogen_kw',
                                                 default=jf.get('jaarpiek_afname_kw', 0)), 0),
        'sitepiek_met_batterij_kw': round(jf.get('jaarpiek_afname_kw', 0), 0),
        'spot_afname_mwh': round(kpi.get('totaal_afname_mwh', 0), 1),
        'spot_injectie_mwh': round(kpi.get('totaal_pv_mwh', 0) - kpi.get('pv_naar_eigen_verbruik_mwh', 0), 1),
    }


def bouw_inv(config):
    """Bouw het INV-object voor het rapport uit de config + sim-JSON's."""
    def laad(p):
        with open(p, encoding='utf-8') as fh:
            return json.load(fh)

    beter = laad(config['beter_json'])
    minder = laad(config['minder_json'])
    beter_onb = laad(config['beter_onbalans_json']) if config.get('beter_onbalans_json') else None

    comp_beter = _factuur_componenten(beter)
    comp_minder = _factuur_componenten(minder)
    en = _energie(beter)
    en['jaarverbruik_gebouw_mwh'] = round(config.get('jaarverbruik_gebouw_mwh', 0), 1)

    besparing = comp_minder['subtotaal'] - comp_beter['subtotaal']
    # windfall = variant3 − variant2 op het betere scenario (indien meegegeven)
    windfall = 0
    if beter_onb is not None:
        windfall = round(comp_beter['subtotaal'] - _factuur_componenten(beter_onb)['subtotaal'])
    windfall = config.get('onbalans_windfall_jaar1', windfall)

    inv = {
        'klant': config.get('klant', ''),
        'locatie': config.get('locatie', ''),
        'datum': config.get('datum', ''),
        'profielen': beter.get('profielen'),  # per-kwartier arrays voor de heatmaps
        'ontwerp': config['ontwerp'],
        'energie': en,
        'facturen': {
            'afrekening_periode_mnd': config.get('afrekening_periode_mnd', 1),
            'afrekening_bedrag': round(config.get('afrekening_bedrag', 0)),
            'bestaand_jaar': round(config['bestaand_jaar']),
            'scherp_dynamisch_jaar': round(config['scherp_dynamisch_jaar']),
            'minder_scenario_jaar': comp_minder['subtotaal'],
            'minder_scenario_label': config.get('minder_scenario_label', 'Basis scenario'),
            'beter_scenario_jaar': comp_beter['subtotaal'],
            'beter_scenario_label': config.get('beter_scenario_label', 'Voorstel (variant 2)'),
        },
        'componenten': {
            'energie': {'minder': comp_minder['energie'], 'beter': comp_beter['energie']},
            'distributie': {'minder': comp_minder['distributie'], 'beter': comp_beter['distributie']},
            'waarvan_capaciteit': {'minder': comp_minder['capaciteit'], 'beter': comp_beter['capaciteit']},
            'heffingen': {'minder': comp_minder['heffingen'], 'beter': comp_beter['heffingen']},
        },
        'besparing_jaar1': besparing,
        'besparing_energie_deel': config.get('besparing_energie_deel', 0.72),
        'onbalans_windfall_jaar1': windfall,
    }
    return inv


def schrijf_rapport(inv, sjabloon_pad, uit_pad):
    """Injecteer window.FLUCTUS_RAPPORT_DATA vóór het hoofd-script in het sjabloon."""
    with open(sjabloon_pad, encoding='utf-8') as fh:
        html = fh.read()
    data_script = ('<script>window.FLUCTUS_RAPPORT_DATA = '
                   + json.dumps(inv, ensure_ascii=False) + ';</script>\n')
    # Injecteer net vóór het EERSTE niet-CDN <script> (het hoofd-script).
    marker = '<script>\n// ='
    idx = html.find(marker)
    if idx == -1:
        # fallback: vóór </body>
        html = html.replace('</body>', data_script + '</body>')
    else:
        html = html[:idx] + data_script + html[idx:]
    with open(uit_pad, 'w', encoding='utf-8') as fh:
        fh.write(html)
    return uit_pad


def voorbeeld_config():
    return {
        "klant": "Voorbeeld NV",
        "locatie": "9220 · Fluvius Midden-Vlaanderen · MS",
        "datum": "juli 2026",
        "beter_json": "sim_beter.json",            # sim-run beter scenario, variant 2
        "minder_json": "sim_minder.json",          # sim-run minder/basis scenario, variant 2
        "beter_onbalans_json": "sim_beter_onbalans.json",  # optioneel: beter scenario variant 3 (windfall)
        "jaarverbruik_gebouw_mwh": 164,
        "afrekening_periode_mnd": 1,
        "afrekening_bedrag": 13600,
        "bestaand_jaar": 163200,                   # extrapolatie huidige afrekening
        "scherp_dynamisch_jaar": 151000,           # zelfde verbruik, scherp dynamisch contract
        "minder_scenario_label": "dynamisch contract zonder PV/batterij, met laadplein → aansluiting verhoogd",
        "beter_scenario_label": "PV + batterij + laadplein, variant 2",
        "besparing_energie_deel": 0.72,
        "onbalans_windfall_jaar1": None,           # None = bereken uit beter_onbalans_json
        "ontwerp": {
            "pv_kwp": 600, "batterij_kw": 300, "batterij_kwh": 600,
            "laadpalen": {"ac": 20, "dc160": 1, "dc400": 0},
            "batterij_cycli_jaar": 500, "batterij_max_cycli": 8000,
            "contract": "Dynamisch (spot passthrough)", "sturing": "Variant 2 (volledig, excl. onbalans)"
        }
    }


def main():
    if len(sys.argv) < 2:
        # geen config → schrijf een voorbeeld-config weg
        with open('rapport_config.voorbeeld.json', 'w', encoding='utf-8') as fh:
            json.dump(voorbeeld_config(), fh, indent=2, ensure_ascii=False)
        print("Geen config meegegeven. Voorbeeld weggeschreven naar rapport_config.voorbeeld.json")
        print("Gebruik: python3 rapport_generator.py config.json [sjabloon.html] [uit.html]")
        return
    config_pad = sys.argv[1]
    sjabloon = sys.argv[2] if len(sys.argv) > 2 else 'financieel-rapport.html'
    with open(config_pad, encoding='utf-8') as fh:
        config = json.load(fh)
    uit = sys.argv[3] if len(sys.argv) > 3 else ('rapport_' +
          (config.get('klant', 'klant').replace(' ', '_')) + '.html')
    inv = bouw_inv(config)
    schrijf_rapport(inv, sjabloon, uit)
    print("Rapport geschreven:", uit)
    print("  besparing jaar 1:", inv['besparing_jaar1'], "€ | windfall:", inv['onbalans_windfall_jaar1'], "€")
    print("  facturen:", {k: inv['facturen'][k] for k in
          ('bestaand_jaar', 'scherp_dynamisch_jaar', 'minder_scenario_jaar', 'beter_scenario_jaar')})


if __name__ == '__main__':
    main()
