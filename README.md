# Automated OLTP-to-OLAP Schema Transformation Using LLMs and Multi-Criteria Pareto Optimisation

Prototype developed for the bachelor thesis of Orkhan Mammadov,
Riga Technical University, 2026. Scientific adviser: Mg.sc.ing. Ainārs Auziņš.

## Files
| File | Purpose |
|---|---|
| setup_tpch.py | Generates the TPC-H database (scale factor 1) |
| run_queries.py | Runs the 22 standard TPC-H queries on the original schema |
| workload.py | 13 logical workload queries, schema descriptions and SQL translator |
| build_star.py | Builds baseline 2: manually designed star schema |
| build_heuristic.py | Builds baseline 3: rule-based heuristic transformation |
| run_workload.py, summary.py | Measures and compares the baseline schemas |
| prototype.py | Rule database, schema builder, evaluator, LLM assessment, Pareto search |
| results_*.csv, llm_cache.json, pareto_front.png | Results of the experiments |

## How to run
1. `pip install duckdb pandas matplotlib anthropic python-dotenv`
2. Create a `.env` file with `ANTHROPIC_API_KEY=...` and `CLAUDE_MODEL=claude-haiku-4-5-20251001`
3. `python setup_tpch.py`, then `python build_star.py` and `python build_heuristic.py`
4. `python run_workload.py` (baselines), then `python prototype.py` (prototype)
