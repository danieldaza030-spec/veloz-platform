"""Project-wide constants: raw source schemas and bucket names.

Schemas here mirror the raw handoff spec in `docs/data-sources.md` exactly —
they describe what each upstream source hands over, not how the platform's
Bronze/Silver/Gold layers reshape it. Schema normalization and any derived
(Bronze/Silver/Gold) schema is the engineer's own work per `CLAUDE.md` and
does not belong in this package.
"""
