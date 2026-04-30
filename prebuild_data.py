"""
Pre-bouw marktdata kwartierreeksen voor de simulator.
Schrijft /tmp/fluctus_markt.json met spot_q, imb_q, solar_norm, profiel_kwartier.
Wordt aangeroepen door server.js bij startup.
"""
import json, sys
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).parent

VAN = datetime(2025, 4, 1)
TOT = datetime(2026, 4, 1)

def load_cache(naam):
    p = BASE / naam
    if not p.exists():
        sys.stderr.write(f"WARN: {naam} niet gevonden\n")
        return {}
    with open(p) as f:
        data = json.load(f)
    return {int(x['t']): x['v'] for x in data}

spot_dict  = load_cache('fluctus-cache-spot.json')
imb_dict   = load_cache('fluctus-cache-imb.json')
solar_dict = load_cache('fluctus-cache-solar.json')

spot_q, imb_q, solar_q = [], [], []
cur = VAN
while cur < TOT:
    ts = int(cur.timestamp() * 1000)
    ht = (ts // 3600000) * 3600000
    spot_q.append(spot_dict.get(ht, 80.0))
    imb_q.append(imb_dict.get(ts, spot_dict.get(ht, 80.0)))
    solar_q.append(solar_dict.get(ts, 0.0))
    cur += timedelta(minutes=15)

solar_sum = sum(solar_q)
solar_norm = [x / solar_sum for x in solar_q] if solar_sum > 0 else solar_q

# Profiel laden
profiel = []
for naam in ['slager.json', 'data/slager.json']:
    p = BASE / naam
    if p.exists():
        with open(p) as f:
            profiel = json.load(f)
        break

out = {
    'spot_q': spot_q,
    'imb_q': imb_q,
    'solar_norm': solar_norm,
    'profiel': profiel,
    'van': VAN.strftime('%Y-%m-%d'),
    'tot': TOT.strftime('%Y-%m-%d'),
    'n_kwartieren': len(spot_q),
}
out_path = '/tmp/fluctus_markt.json'
with open(out_path, 'w') as f:
    json.dump(out, f)

sys.stderr.write(f"OK: {len(spot_q)} kwartieren, spot gem={sum(spot_q)/len(spot_q):.1f} EUR/MWh\n")
print(out_path)
