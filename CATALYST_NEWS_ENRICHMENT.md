# Expansion Catalyst News Enrichment

Dataset: `dataset-catalyst-news-enrichment-2026-07-19-expansion-v1`

Status: inspected `READY` for secondary full-content coverage only

Manifest: `154e5831aba1912ddaf3bd66af0ea2d5cc26f5a211c7a6292d6ffc88514fff2e`

This contract freezes all 1,987 expansion pairs, 100 dates, and 7,391 unique
Alpaca/Benzinga discovery articles before requerying the identical four-day
windows through 09:35 ET with `include_content=true`.

Collection checkpoints each date, uses same-provider retries only, preserves
missing articles and missing bodies explicitly, and stores symbols, metadata,
and content only under `LOCAL_HISTORICAL_DATA_ROOT`. The existing selection,
discovery index, collector, provider adapter, API parameters, cutoff, and disk
reserve are hash-bound.

Full Benzinga content remains secondary discovery evidence. This dataset cannot
verify a primary catalyst, satisfy independent analyst corroboration, read a
target outcome, invent a strategy variant, or change production.

All 100 dates completed with zero errors. Every one of the 7,391 unique
discovery articles was returned; 4,205 contained a full body totaling
27,772,306 bytes. At least one content-complete article covers 1,183 pairs, while
804 pairs have no content-complete lead. All 8,693 date-attributed article keys
were returned and 5,128 date attributions contained content.

The body-content count exactly matches the earlier summary-availability count,
so the provider did not recover bodies for the 3,186 metadata-only articles.
Those records remain missing content; they are not dropped, inferred, or
requeried through another provider.

```sh
python3 catalyst_news_enrichment.py freeze
python3 catalyst_news_enrichment.py collect --manifest \
  historical_batches/catalyst_news_enrichment/manifests/dataset-catalyst-news-enrichment-2026-07-19-expansion-v1-154e5831aba1912ddaf3bd66af0ea2d5cc26f5a211c7a6292d6ffc88514fff2e.json
python3 catalyst_news_enrichment.py inspect --manifest \
  historical_batches/catalyst_news_enrichment/manifests/dataset-catalyst-news-enrichment-2026-07-19-expansion-v1-154e5831aba1912ddaf3bd66af0ea2d5cc26f5a211c7a6292d6ffc88514fff2e.json
```
