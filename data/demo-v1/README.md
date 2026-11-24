# Demo dataset

| Release | Value |
| --- | --- |
| Dataset | `demo-v1`; synthetic retail data; EUR and UTC |
| Contents | 1 historical + 2 daily + 8 optional scenario deliveries; 11 manifests, 22 Parquet files |
| Run order | `historical/snapshot-001`, then `incremental/daily-001`, then `incremental/daily-002` |
| Each daily delivery | 153 rows; 151 inserts, 2 updates, no rejects |
| Documentation | [Data contract](../../docs/data_contract.md); [setup](../../README.md#quickstart) |
| Full release SHA-256 | `9ddd287e2a5efc6217f93123def9479c960a88e9a40f0293476c864213edc534` |

| Checkpoint | Customers | Products | Orders | Items | Net sales |
| --- | ---: | ---: | ---: | ---: | ---: |
| Snapshot | 800 | 50 | 4,900 | 8,747 | €378,216.35 |
| Day 1 | 801 | 50 | 4,950 | 8,847 | €382,355.79 |
| Day 2 | 802 | 50 | 5,000 | 8,947 | €386,849.69 |

- Start each scenario from Day 2 in separate core/analytics schemas, except failure/retry, which starts from Day 1.
- Delivery paths below are relative to this directory; each contains `manifest.json`.

| Scenario | Delivery | Expected result |
| --- | --- | --- |
| Replay | `incremental/daily-001` | `already_applied`; no extra ledger row |
| Duplicates | `scenarios/duplicates/duplicate-001` | 2 duplicates; unchanged core and sales |
| Late arrival | `scenarios/late-arrival/late-001` | Insert 1 order and 1 item; add €12.00 to 9 November; total €386,861.69 |
| Stale version | `scenarios/stale-version/stale-001` | 1 stale row; product price stays €8.50 |
| Conflicts, run separately | `scenarios/version-conflict/conflict-batch-001` or `scenarios/version-conflict/conflict-current-001` | Fail; unchanged core; no applied entry |
| Invalid rows | `scenarios/invalid-rows/invalid-001` | Quarantine 2 orders and 6 items; unchanged core and sales; replay keeps 8 rejects |
| Zero discount | `scenarios/schema-evolution/discount-zero-replay-001` | 1 duplicate; unchanged core and sales |
| Discount correction | `scenarios/schema-evolution/discount-001` | Update 2 items; order `order-004901` falls from €21.00 to €18.00; total €386,846.69 |
| Failure/retry | `incremental/daily-002` | Inject `before-core`, `before-commit` or `after-commit`; retry without the flag reaches Day 2 once |

- Verify core counts, `quarantined_events`, `applied_batches` and published `mart_daily_sales`.
