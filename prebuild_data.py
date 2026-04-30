"""
Pre-bouw marktdata kwartierreeksen voor de simulator.
Schrijft /tmp/fluctus_markt.json met spot_q, imb_q, solar_norm, profiel_kwartier.

Cache-bestanden staan in de aparte GitHub repo JohanMMK/market-data/data/
Ze worden gedownload via GitHub raw URL als ze niet lokaal gevonden worden.
"""
import json, sys, os
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).parent

GITHUB_RAW = "https://raw.githubusercontent.com/JohanMMK/market-data/main/data"
CACHE_DIR  = Path("/tmp/fluctus-cache")
CACHE_DIR.mkdir(exist_ok=True)

def load_cache(naam):
    # 1. Zoek lokaal in de repo (voor lokale dev)
    for zoekpad in [
        BASE / 'market-data' / 'data' / naam,
        BASE / 'data' / naam,
        BASE / naam,
    ]:
        if zoekpad.exists():
            with open(zoekpad) as f:
                data = json.load(f)
            sys.stderr.write(f"OK (lokaal): {zoekpad} ({len(data)} entries)\n")
            return {int(x['t']): x['v'] for x in data}, [int(x['t']) for x in data]

    # 2. Download van GitHub raw
    cached = CACHE_DIR / naam
    if not cached.exists():
        url = f"{GITHUB_RAW}/{naam}"
        sys.stderr.write(f"Download: {url}\n")
        try:
            import urllib.request
            urllib.request.urlretrieve(url, cached)
            sys.stderr.write(f"OK: {cached} ({cached.stat().st_size//1024} KB)\n")
        except Exception as e:
            sys.stderr.write(f"FOUT bij download {naam}: {e}\n")
            return {}, []
    else:
        sys.stderr.write(f"OK (cache): {cached}\n")

    with open(cached) as f:
        data = json.load(f)
    return {int(x['t']): x['v'] for x in data}, [int(x['t']) for x in data]


def laatste_volledige_dag(timestamps_ms):
    if not timestamps_ms:
        return None
    last_dt = datetime.fromtimestamp(max(timestamps_ms) / 1000, tz=timezone.utc)
    if last_dt.hour == 23 and last_dt.minute == 45:
        return last_dt.date()
    else:
        return (last_dt - timedelta(days=1)).date()


# Laad de drie caches
spot_dict,  spot_ts  = load_cache('fluctus-cache-spot.json')
imb_dict,   imb_ts   = load_cache('fluctus-cache-imb.json')
solar_dict, solar_ts = load_cache('fluctus-cache-solar.json')

# Bepaal de meest beperkende laatste volledige dag
laatste_spot  = laatste_volledige_dag(spot_ts)
laatste_imb   = laatste_volledige_dag(imb_ts)
laatste_solar = laatste_volledige_dag(solar_ts)
sys.stderr.write(f"Laatste volledige dag — spot: {laatste_spot}, imb: {laatste_imb}, solar: {laatste_solar}\n")

beschikbare = [d for d in [laatste_spot, laatste_imb, laatste_solar] if d is not None]
if not beschikbare:
    sys.stderr.write("WARN: geen cache gevonden — gebruik fallback\n")
    from datetime import date
    laatste_dag = date.today() - timedelta(days=1)
else:
    laatste_dag = min(beschikbare)

# Periode: van = laatste_dag - 365 dagen, tot = laatste_dag + 1 (exclusief)
TOT_MARKT = datetime(laatste_dag.year, laatste_dag.month, laatste_dag.day, tzinfo=timezone.utc) + timedelta(days=1)
VAN_MARKT = TOT_MARKT - timedelta(days=365)
sys.stderr.write(f"Simulatieperiode: {VAN_MARKT.date()} → {(TOT_MARKT-timedelta(days=1)).date()} (inclusief)\n")

# Spot + imbalance
spot_q, imb_q = [], []
cur = VAN_MARKT
while cur < TOT_MARKT:
    ts = int(cur.timestamp() * 1000)
    ht = (ts // 3600000) * 3600000
    spot_q.append(spot_dict.get(ht, 80.0))
    imb_q.append(imb_dict.get(ts, spot_dict.get(ht, 80.0)))
    cur += timedelta(minutes=15)
sys.stderr.write(f"Markt: {len(spot_q)} kwartieren, spot gem={sum(spot_q)/len(spot_q):.1f} EUR/MWh\n")

# Solar norm: volledig kalenderjaar 2025
VAN_SOLAR = datetime(2025, 1, 1, tzinfo=timezone.utc)
TOT_SOLAR = datetime(2026, 1, 1, tzinfo=timezone.utc)
solar_q = []
cur = VAN_SOLAR
while cur < TOT_SOLAR:
    solar_q.append(solar_dict.get(int(cur.timestamp() * 1000), 0.0))
    cur += timedelta(minutes=15)
solar_sum = sum(solar_q)
solar_norm = [x / solar_sum for x in solar_q] if solar_sum > 0 else [0.0] * len(solar_q)
sys.stderr.write(f"Solar 2025: {len(solar_norm)} kwartieren, niet-nul={sum(1 for v in solar_norm if v > 0)}\n")

# Profiel
profiel = []
for naam in ['data/profielen/Slager.json', 'data/profielen/slager.json']:
    p = BASE / naam
    if p.exists():
        with open(p) as f:
            profiel = json.load(f)
        sys.stderr.write(f"Profiel geladen: {p}\n")
        break

out = {
    'spot_q':           spot_q,
    'imb_q':            imb_q,
    'solar_norm':       solar_norm,
    'profiel':          profiel,
    'van':              VAN_MARKT.strftime('%Y-%m-%d'),
    'tot':              (TOT_MARKT - timedelta(days=1)).strftime('%Y-%m-%d'),
    'n_kwartieren':     len(spot_q),
    'solar_kwartieren': len(solar_norm),
}

with open('/tmp/fluctus_markt.json', 'w') as f:
    json.dump(out, f)

sys.stderr.write(f"OK: /tmp/fluctus_markt.json — {out['van']} t/m {out['tot']}, {out['n_kwartieren']} kwartieren\n")
print('/tmp/fluctus_markt.json')
