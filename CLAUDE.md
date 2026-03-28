# CLAUDE.md — HARPY
_Last updated: 2026-03-28_

---

## Rules of Engagement

**Every Claude in every session must do this:**

1. Read this file before starting any work
2. Read `ROADMAP.md` — know what's Now, what's Next
3. Check the file structure section against `git status` and actual repo — flag discrepancies before touching code
4. Update this file in the same commit if anything structural changes (schema, sources, scoring, file patterns)
5. Update `ROADMAP.md` when a task completes, a new idea surfaces, or priorities shift
6. Nothing is done until it is committed and pushed. "I built X" without a push didn't happen.

**If a session ends mid-task without pushing:** the next Claude's first job is `git status`, understand what's in flight, and either finish it or explicitly mark it abandoned in ROADMAP.md.

**Push back when:**
- Asked to build something not in ROADMAP.md without a reason
- The codebase diverges from what this file describes
- A task would introduce duplication that already exists in `utils.py`
- Record counts or statuses in this file look stale

---

## What We're Building

A personal feed of apparatus actions — procurement filings, arms sales,
sanctions designations, lobbying activity — surfaced from public government
data before any mainstream source covers them.

This is not a news aggregator. Not a prediction engine. Not a map.
It is a feed. Each item is something the machine did that you weren't told about.

## The Thesis

The apparatus has to do paperwork before it does anything else.
Procurement solicitations, contract awards, arms sale notifications,
regulatory filings. That paperwork is public. Nobody reads it.

Actions precede statements. Statements precede news.
Watch what the machine does, not what it says.

RSS captures statements. We do not use RSS.

## North Star

HARPY surfaces something you didn't already know, before you could have
known it from any mainstream source.

That is the only metric that matters. A feature exists only if it
demonstrably moves toward that metric.

---

## File Structure

Pattern for signal files: `data/{source}_signals.json`
Pattern for diff baselines: `data/{source}_baseline.json` or `data/{source}_known_uids.json`

```
harpy/
  index.html                              ← frontend: map + convergence clusters + feed
  CLAUDE.md                               ← ground truth (this file)
  ROADMAP.md                              ← backlog and current priorities
  data/
    signals.json                          ← merged, enriched pipeline output (source of truth for frontend)
    profiles/{ISO2}.json                  ← per-country structural profiles
    dsca_signals.json
    dsca_nato.json                        ← NATO/NSPA notifications (not yet in pipeline)
    fara_signals.json
    lda_signals.json
    federalregister_signals.json
    imf_signals.json
    anchor_signals.json
    ofac_signals.json
    ofac_known_uids.json                  ← OFAC diff baseline
    bis_signals.json
    bis_baseline.json                     ← BIS diff baseline
    sam_signals.json
  scripts/
    utils.py                              ← canonical shared utilities — import from here, not inline
    build_signals.py                      ← merge + enrich → signals.json
    fetch_dsca.py
    fetch_fara.py
    fetch_lda.py
    fetch_federalregister.py
    fetch_imf.py
    fetch_anchor.py
    fetch_ofac.py
    fetch_bis.py
    fetch_sam.py
    country_data.py                       ← source data for profile generation
    generate_profiles.py                  ← writes profiles/{ISO2}.json
  .github/
    workflows/
      fetch.yml                           ← daily cron pipeline
```

When adding a new source: add `fetch_{source}.py`, `data/{source}_signals.json` appears automatically. Update `SOURCE_LAYER_MAP` and `SOURCE_QUALITY` in `utils.py`. Update this file.

---

## Signal Schema

Every record in `signals.json` is enriched by `build_signals.py` at build time:

```json
{
  "iso": "SA",
  "source": "dsca",
  "signal_date": "2026-01-15",
  "title": "Saudi Arabia — PATRIOT Missile System",
  "value_usd": 9000000000.0,
  "description": "...",
  "profile": {
    "name": "Saudi Arabia",
    "score": 8,
    "factors": ["chokepoint", "hydrocarbons", "active conflict"],
    "rationale": "..."
  },
  "layer": "military",
  "quality": 1.0,
  "dollar_mod": 1.3,
  "is_fr_policy": null
}
```

Rules:
- `raw_score` and `weight` are stripped at build time — do not write them in fetch scripts
- `profile` is null if no profile exists for the iso
- `is_fr_policy` is true/false for federalregister signals, null for all others
- `build_signals.py` filters: drops iso null/US/XX, drops signal_date before 2025-01-01
- Source-specific extras (e.g. `cn_number`, `uid`, `filing_uuid`) are preserved
- All scoring inputs are precomputed — `index.html` reads them directly, does not recompute

---

## utils.py — Shared Utilities

All fetch scripts must import from here. Never duplicate locally.

- `COUNTRY_NAME_TO_ISO2` — canonical lowercase name → ISO alpha-2
- `ALPHA3_TO_ALPHA2` — ISO alpha-3 → alpha-2 (SAM and similar)
- `SOURCE_LAYER_MAP` — source → layer
- `source_quality(source, title)` — quality weight
- `dollar_modifier(value_usd, source)` — log-scaled bonus (DSCA/SAM only)
- `load_profile(iso2)`, `profile_score(iso2)` — profile file I/O
- `append_and_write(path, source, new_signals, dedup_key_fn)` — standard accumulating output
- `write_error(path, source, error)` — preserve existing signals on fetch failure

---

## Source Layer Taxonomy

| Layer | Sources | Color |
|---|---|---|
| military | dsca, anchor_budget | `#4a9eff` |
| influence | fara, lda | `#ff6b35` |
| regulatory | federalregister, ofac, bis | `#c084fc` |
| procurement | sam | `#34d399` |
| financial | imf, cftc | `#f59e0b` |
| legislative | *(reserved)* | `#64748b` |
| adversarial | *(reserved)* | `#ef4444` |

Colors are assigned per layer in `index.html` via `LAYER_COLORS` / `layerColor(sig.layer)`.

---

## Source Quality Weights

Defined in `utils.py` `SOURCE_QUALITY`. Applied by `source_quality(source, title)`.

| Source | Quality | Rationale |
|---|---|---|
| dsca | 1.0 | Formal congressional notification |
| ofac | 0.9 | Direct government designation |
| fara | 0.9 | Legally required foreign agent disclosure |
| bis | 0.85 | Export control designation |
| federalregister (policy) | 0.8 | Matches sanctions/export/defense regex |
| anchor_budget | 0.75 | Single contractor 6-K, not a government action |
| sam | 0.7 | Procurement solicitation |
| imf | 0.5 | Financial monitoring, soft signal |
| lda | 0.5 | Lobbying disclosure — precursor, not action |
| cftc | 0.55 | Market positioning — correlate, not paperwork |
| federalregister (other) | 0.2 | Routine/cultural notices |

---

## Diff Pipelines (OFAC, BIS)

Trip-wires, not feeds. First run establishes a baseline. Subsequent runs emit
only new entries. Zero signals is the normal state — it means nothing new was
designated. Do not treat zero output as broken.

---

## Convergence Scoring

The cluster panel scores countries where multiple signal layers overlap in 30 days.

```
score = log2(structuralScore + 1) × lift × convergence × recencyWeight × temporalBonus
convergence = Σ(layerContribution) × LAYER_BONUS[layerCount - 1]
layerContribution = max(quality × recencyDecay × dollar_mod) per layer
```

Eligibility: ≥2 distinct layers, structural score ≥4 (or ≥2 with 3+ layers).
FR signals only count toward a layer if `is_fr_policy === true`.
All inputs are precomputed by `build_signals.py` — `index.html` does not recompute them.

---

## Country Profiles

`data/profiles/{ISO2}.json`. Generated by `generate_profiles.py` from `country_data.py`.

**Structural interest score rubric — additive, capped at 10:**

| Factor | Points |
|---|---|
| Controls a major maritime chokepoint | +3 |
| Major hydrocarbon reserves or transit | +2 |
| Active conflict or occupied territory | +2 |
| Nuclear program (active or latent) | +2 |
| Strategic minerals — deposits or processing | +2 |
| Western Hemisphere presence | +1 |
| Major US basing or force presence | +1 |
| Significant Chinese BRI/debt exposure | +1 |
| Active sanctions regime | +1 |
| Regional power or proxy hub | +1 |

Scores above 7 are rare and should feel rare.

---

## Aesthetic

```css
--bg:     #363639
--hdr:    #232324
--sep:    #3a3a3c
--text:   #f5f5f7
--muted:  #98989f
--mono:   ui-monospace, 'SF Mono', 'Consolas', 'Monaco', monospace
```

Monospace. No gradients. No rounded corners. Dark intelligence terminal.
