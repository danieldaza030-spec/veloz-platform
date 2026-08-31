# Bronze layer conventions

## Quarantine

Bad records detected while ingesting a raw source into Bronze are written
to a Delta table under the existing `bronze-veloz` bucket, not a separate
bucket:

```
s3a://bronze-veloz/_quarantine/<source>/
```

`<source>` is the raw source name, e.g. `orders`, `fulfillment`,
`payments`. Writes are appends — quarantine records accumulate over time
and are never overwritten by a later run for a different date.

### Schema

| Column | Type | Meaning |
|---|---|---|
| `source` | string | Raw source the record came from, e.g. `"fulfillment"`. |
| `partition_date` | string (`YYYY-MM-DD`) | Extract/partition date the bad record belongs to. |
| `key` | string | S3 key or row identifier that's bad. |
| `reason` | string | Why the record was quarantined. |
| `detected_at` | timestamp | When the record was quarantined. |
| `raw_snippet` | string, nullable | Raw content for debugging; null when not applicable (e.g. a missing file). |

### Reason values in use

- `missing_file` — an expected raw file for the partition wasn't found.
- `malformed_row` — a row failed schema validation or a business rule.

New reason values can be added as needed; there's no fixed enum.

See `infrastructure/quarantine_writer.py` for the writer and
`metadata/quarantine_schema.py` for the schema definition.
