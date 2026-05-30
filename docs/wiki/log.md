# Wiki Log

Append-only chronological record of all wiki operations.

---

2026-05-30 | init | Created wiki structure. Pre-populated pages from codebase knowledge: architecture, hacks, post-mortems, decisions, concepts.
2026-05-30 | session-2 | Coverage page enhancements (country dropdown, 5 cell states, jump-to-active, live JS highlight 5s poll, rotated topic headers). Pipeline done-pair pre-filter: get_fully_processed_pairs() skips pairs where search_cache exists + all cache_pages carry current extract fingerprint. on_pair_start callback propagated through run_pipeline → _run_full / _run_ai_only; sets app_state.current_city/current_topic. 290 Swedish municipalities completed. search_ttl_days set to 3650 (index world first). Email notifications via Resend on 4 routes.
