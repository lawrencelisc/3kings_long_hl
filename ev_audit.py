"""
ev_audit.py
===========
One-off statistical audit of the live regime CSVs.

Goal: determine — from REAL DATA — whether the current V5-EV gates
actually have positive predictive value on BTC forward returns, and
estimate the expected per-trade EV after fees.

Outputs to stdout only. No files written. No side effects.
"""
import pandas as pd
import numpy as np
from pathlib import Path

# ── Load + sort + dedupe all three regime CSVs ─────────────────────────────
ROOT  = Path(__file__).parent
files = sorted(ROOT.glob("status/btc_regime_long_hl*.csv"))
frames = []
for f in files:
    try:
        df = pd.read_csv(f)
        frames.append(df)
    except Exception as e:
        print(f"skip {f.name}: {e}")
df = pd.concat(frames, ignore_index=True)

df = df[pd.to_numeric(df['btc_price'], errors='coerce').notna()]
df['btc_price']   = df['btc_price'].astype(float)
df = df[df['btc_price'] > 1000].reset_index(drop=True)  # drop the 0-price corruption rows
df['signal_code'] = pd.to_numeric(df['signal_code'], errors='coerce').fillna(0).astype(int)
df['adx']         = pd.to_numeric(df['adx'], errors='coerce')
df['ndipdi']      = pd.to_numeric(df['ndipdi'], errors='coerce')
df['ndi_rising']  = pd.to_numeric(df['ndi_rising'], errors='coerce')   # 1 if NOT 5m peak (anti-peak pass)
df['pdi_rising']  = pd.to_numeric(df['pdi_rising'], errors='coerce')   # 1 if NOT 30m extended (anti-chase pass)
df['ndi_slope']   = pd.to_numeric(df['ndi_slope'], errors='coerce')    # =ΔADX(25m) in V5.1 schema
df['pdi_slope']   = pd.to_numeric(df['pdi_slope'], errors='coerce')    # =BTC 30m pump %
df['ema_dir']     = pd.to_numeric(df['ema_dir'], errors='coerce')      # 1 if legacy L5 (PDI dominant) would pass
df['is_bear']     = pd.to_numeric(df['is_bear'], errors='coerce')
df['ts']          = pd.to_datetime(df['timestamp'], errors='coerce')
df = df.dropna(subset=['ts', 'btc_price']).sort_values('ts').reset_index(drop=True)

# Median sample period (in seconds)
gaps_s = df['ts'].diff().dt.total_seconds().dropna()
median_gap_s = float(gaps_s.median())
print(f"rows={len(df)} | span={df['ts'].iloc[0]} → {df['ts'].iloc[-1]}")
print(f"median sample gap = {median_gap_s:.1f}s")

# ── Build forward-return columns at 5/15/30/60-min horizons ────────────────
# To compute "BTC return in next 30min" without relying on uneven sampling,
# we do an asof-merge: for each ts, find the row with ts >= ts+H and take
# the realized BTC return.
df_sorted = df[['ts', 'btc_price']].sort_values('ts').reset_index(drop=True)

def realized_fwd_return(horizon_min: int) -> pd.Series:
    """For each row, fwd return = (price at first ts >= ts+H) / price now - 1."""
    target = df_sorted['ts'] + pd.Timedelta(minutes=horizon_min)
    pos    = np.searchsorted(df_sorted['ts'].values, target.values, side='left')
    # If beyond array, mark NaN
    valid       = pos < len(df_sorted)
    fwd_prices  = np.where(valid, df_sorted['btc_price'].values[np.clip(pos, 0, len(df_sorted)-1)], np.nan)
    return pd.Series((fwd_prices / df_sorted['btc_price'].values) - 1.0)

df['fwd_5m']  = realized_fwd_return(5)
df['fwd_15m'] = realized_fwd_return(15)
df['fwd_30m'] = realized_fwd_return(30)
df['fwd_60m'] = realized_fwd_return(60)

# Drop rows with extreme outliers ( > 50% move = likely data glitch)
for c in ['fwd_5m','fwd_15m','fwd_30m','fwd_60m']:
    df[c] = df[c].where(df[c].abs() < 0.5)

print("\n=== Forward-return summary (BTC) ===")
for h in ['fwd_5m', 'fwd_15m', 'fwd_30m', 'fwd_60m']:
    x = df[h].dropna()
    if len(x):
        print(f"{h}: mean={x.mean()*100:+.4f}% | median={x.median()*100:+.4f}% "
              f"| std={x.std()*100:.4f}% | n={len(x)}")


def ev_summary(label: str, mask: pd.Series, horizon: str = 'fwd_30m',
               tp_pct: float = None, sl_pct: float = None, rt_cost: float = 0.0019) -> dict:
    """Compute summary stats and (if tp/sl) a simulated trade EV using barrier exit."""
    sub = df.loc[mask, [horizon]].dropna()
    n   = len(sub)
    if n == 0:
        print(f"  {label}: n=0 (no data)")
        return {}
    r   = sub[horizon].values
    out = {
        'label':     label,
        'n':         n,
        'mean':      r.mean(),
        'median':    np.median(r),
        'wr_raw':    float((r > 0).mean()),
        'wr_post_fee': float((r > rt_cost).mean()),
        'p25': float(np.percentile(r, 25)),
        'p75': float(np.percentile(r, 75)),
    }
    print(f"  {label:38s} n={n:5d} | "
          f"mean={out['mean']*100:+.3f}% | "
          f"WR_raw={out['wr_raw']*100:5.1f}% | "
          f"WR_postfee={out['wr_post_fee']*100:5.1f}% | "
          f"p25={out['p25']*100:+.3f}% | p75={out['p75']*100:+.3f}%")
    return out


# ── 1. Baseline: all rows ──
print("\n=== Forward 30m BTC return: regime gate effectiveness ===")
ev_summary("ALL rows",                       df.index.notnull() & df.index.notnull(),)  # everything
ev_summary("signal_code=1 (V5 GREEN)",       df['signal_code'] == 1)
ev_summary("signal_code=0 (V5 BLOCKED)",     df['signal_code'] == 0)

# ── 2. Decompose each gate independently ──
print("\n=== Per-gate forward 30m return (each is the SOLE pass condition) ===")
ev_summary("NOT at 5m peak (anti-peak only)",   df['ndi_rising'] == 1)
ev_summary("AT 5m peak     (anti-peak BLOCK)",  df['ndi_rising'] == 0)
ev_summary("NOT 30m extended (anti-chase only)", df['pdi_rising'] == 1)
ev_summary("30m extended    (anti-chase BLOCK)", df['pdi_rising'] == 0)
ev_summary("NOT bear",                          df['is_bear'] == 0)
ev_summary("Bear",                              df['is_bear'] == 1)
ev_summary("Legacy L5 PASS (PDI dominant)",     df['ema_dir'] == 1)
ev_summary("Legacy L5 BLOCK",                   df['ema_dir'] == 0)

# ── 3. Buy-the-dip benchmark referenced in header ──
print("\n=== Mean-reversion benchmark (BTC 5m drop > 0.5% → fwd 30m) ===")
ev_summary("BTC 5m < -0.5% (dip)",  df['pdi_slope'].notna() & (df['pdi_slope'] < -0.5))
ev_summary("BTC 5m < -0.3% (mild dip)", df['pdi_slope'].notna() & (df['pdi_slope'] < -0.3))
ev_summary("BTC 5m -0.3 .. +0.3% (flat)", df['pdi_slope'].notna() & (df['pdi_slope'].between(-0.3, 0.3)))

# Note: pdi_slope is reused in V5.1 schema as BTC 30m pump %, but in the regime
# function it's always written as btc_30m_pump * 100. We should also bucket by it.
print("\n=== BTC 30m pump bucket → fwd 30m return ===")
bins = [-100, -2, -1, -0.5, 0, 0.5, 1, 1.5, 2, 3, 100]
df['btc_30m_bucket'] = pd.cut(df['pdi_slope'], bins)
for b, grp in df.groupby('btc_30m_bucket', observed=True):
    x = grp['fwd_30m'].dropna()
    if len(x) < 20:  # need decent sample
        continue
    print(f"  BTC 30m∈{b}: n={len(x):4d} | mean fwd30m={x.mean()*100:+.3f}% | "
          f"WR={(x>0).mean()*100:.1f}% | WR_postfee={(x>0.0019).mean()*100:.1f}%")

# ── 4. SIMULATED trade barrier EV (TP/SL race) ──
# Approximation: for each candidate entry row, look at fwd 5m, 15m, 30m, 60m BTC
# returns and ASSUME a symbol trades 1:1 with BTC (β=1, conservative for majors).
# Apply TP=+P / SL=-Q barriers (in %) and pick whichever hits first within timeout.
# This is approximate — but for FILTER COMPARISON it's apples-to-apples.
print("\n=== Simulated barrier-race EV (β=1 BTC proxy; horizon=30m) ===")

def simulate_barrier(mask: pd.Series, tp: float, sl: float, rt_cost: float = 0.0019):
    """tp/sl in decimal (e.g. 0.0075 for +0.75%)."""
    sub = df.loc[mask].dropna(subset=['fwd_5m', 'fwd_15m', 'fwd_30m'])
    # Pessimistic barrier touch check using path waypoints (5/15/30m only — coarse).
    n = len(sub); wins = losses = timeouts = 0; pnl = 0.0
    for _, row in sub.iterrows():
        path = [row['fwd_5m'], row['fwd_15m'], row['fwd_30m']]
        hit_tp = hit_sl = False
        for r in path:
            if r >= tp: hit_tp = True; break
            if r <= -sl: hit_sl = True; break
        if hit_tp:
            wins += 1; pnl += tp - rt_cost
        elif hit_sl:
            losses += 1; pnl += -sl - rt_cost
        else:
            timeouts += 1
            r_final = path[-1]
            pnl += r_final - rt_cost
    if n == 0:
        return
    ev_per_trade = pnl / n
    wr = (wins) / n
    print(f"  TP={tp*100:.2f}% SL={sl*100:.2f}% | n={n} | "
          f"WR={wr*100:.1f}% | timeouts={timeouts} | "
          f"avg PnL/trade={ev_per_trade*100:+.3f}% (post-fee)")

# ATR-derived TP/SL: HL majors 5m ATR% ≈ 0.10–0.20%; with TP=1.5×ATR / SL=1.0×ATR
# ⇒ typical TP/SL pair ≈ ±0.15%/0.10% up to ±0.30%/0.20%. Test both.
print("\n  --- V5 sizing (TP=1.5×ATR, SL=1.0×ATR), typical ATR%≈0.10-0.20% ---")
for tp, sl in [(0.0015, 0.0010), (0.0023, 0.0015), (0.0030, 0.0020), (0.0045, 0.0030)]:
    simulate_barrier(df['signal_code'] == 1, tp, sl)
print("  --- Same on RAW universe (no gate) ---")
for tp, sl in [(0.0015, 0.0010), (0.0023, 0.0015), (0.0030, 0.0020), (0.0045, 0.0030)]:
    simulate_barrier(df.index.notnull(), tp, sl)

# ── 5. Buy-the-dip variant (counter-trend) ──
print("\n  --- Buy-the-dip (BTC 5m < -0.3%) with same sizing ---")
mask_dip = df['pdi_slope'].notna() & (df['pdi_slope'] < -0.3)
for tp, sl in [(0.0015, 0.0010), (0.0023, 0.0015), (0.0030, 0.0020), (0.0045, 0.0030)]:
    simulate_barrier(mask_dip, tp, sl)

# ── 6. Adverse-selection diagnostic: how often does signal_code=1 LOSE > fee threshold? ──
print("\n=== Adverse-selection rate (signal=1 but loses > 0.30% in 30m) ===")
sub = df[df['signal_code']==1].dropna(subset=['fwd_30m'])
if len(sub):
    n  = len(sub)
    pp = (sub['fwd_30m'] > 0.003).mean()
    pn = (sub['fwd_30m'] < -0.003).mean()
    print(f"  n={n} | >+0.3%={pp*100:.1f}% | <-0.3%={pn*100:.1f}% | mean={sub['fwd_30m'].mean()*100:+.3f}%")

# ── 7. By time-of-day (UTC hour) ──
print("\n=== EV by hour-of-day (UTC) on signal_code=1 (n>=5) ===")
sub = df[df['signal_code']==1].copy()
sub['hr'] = sub['ts'].dt.hour
for h, grp in sub.groupby('hr'):
    if len(grp) < 5: continue
    x = grp['fwd_30m'].dropna()
    if len(x):
        print(f"  hr={h:02d}: n={len(x):3d} | mean={x.mean()*100:+.3f}% | WR={(x>0).mean()*100:.1f}%")

print("\n=== Stats by signal_code × is_bear interaction ===")
for s in [0, 1]:
    for b in [0, 1]:
        m = (df['signal_code']==s) & (df['is_bear']==b)
        x = df.loc[m, 'fwd_30m'].dropna()
        if len(x) < 20: continue
        print(f"  sig={s} bear={b}: n={len(x):4d} mean={x.mean()*100:+.3f}% WR={(x>0).mean()*100:.1f}%")

# ── 8. Discovery: hour-of-day filter on ENTIRE dataset (not just signal=1) ──
print("\n=== Hour-of-day forward 30m return (ALL rows, n>=30) ===")
df['hr'] = df['ts'].dt.hour
for h, grp in df.groupby('hr'):
    x = grp['fwd_30m'].dropna()
    if len(x) < 30: continue
    print(f"  hr={h:02d}: n={len(x):4d} | mean={x.mean()*100:+.3f}% | "
          f"WR={(x>0).mean()*100:.1f}% | WR_postfee={(x>0.0019).mean()*100:.1f}% "
          f"| p75={np.percentile(x,75)*100:+.3f}%")

# ── 9. Two strongest single filters: 30m bucket + hour-of-day combo ──
print("\n=== Combo: BTC 30m∈(0%, +1.5%] AND hour∈{6,7,8,9,10} ===")
m_30m = df['pdi_slope'].between(0.0, 1.5)
m_hr  = df['hr'].isin([6, 7, 8, 9, 10])
ev_summary("ONLY this combo (no V5 gates)", m_30m & m_hr)
ev_summary("ONLY this combo + V5 signal=1", m_30m & m_hr & (df['signal_code']==1))
ev_summary("V5 signal=1 only (for ref)",    df['signal_code']==1)
ev_summary("Combo + NOT 5m extreme dip (5m>-0.5%)",
           m_30m & m_hr & df['pdi_slope'].notna() &
           (df.assign(_5m=df['btc_price'].pct_change(1).fillna(0))['_5m'] > -0.005))

# ── 10. Cost diagnostic: how often is the fwd 30m move LARGER than 2x RT cost? ──
print("\n=== Cost sensitivity: probability fwd 30m move > N×RT cost (RT=0.18%) ===")
rt = 0.0018
for mult, lbl in [(1, "1×RT"), (2, "2×RT"), (3, "3×RT"), (4, "4×RT")]:
    p_pos = (df['fwd_30m'] >  rt*mult).mean()
    p_neg = (df['fwd_30m'] < -rt*mult).mean()
    print(f"  >|{mult}×RT|={mult*rt*100:.3f}%: P(fwd>+{mult*rt*100:.3f}%)={p_pos*100:.1f}% | "
          f"P(fwd<-{mult*rt*100:.3f}%)={p_neg*100:.1f}%")
