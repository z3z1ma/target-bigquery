# CLI configuration contract

Base: `a82b9ba355c996a0577a4ec90a0bffbf5e544ed2`.

## Evidence and correction

A representative typed JSON configuration (nested options, transforms, booleans,
integers and an upsert pattern list) successfully starts the real Click CLI and
completes empty-input EOF. A direct `--config ENV` profile also correctly parses
ordinary scalars and nested JSON objects. The installed console entry point was
separately invoked against the source checkout with synthetic project/dataset
names and empty stdin; it exited successfully without constructing sinks.
These are startup checks, not authentication or ingestion tests.

The second profile exposed an actual selector mismatch. The SDK's environment
parser detects the boolean branch of the boolean/string/array schema for
`upsert`, `overwrite` and `dedupe_before_upsert`. Each CSV pattern
`orders*,!orders_history` became False. Three before-fix CLI regressions failed:
`orders_current` was not selected, despite the documented pattern contract.

The narrow override calls the superclass parser first, then restores raw pattern
text for only those three settings. Existing `selection_matches` handles CSV and
JSON pattern arrays. Boolean spellings retain the SDK's decision: ordinary
true/false aliases are unchanged, and inputs such as `y` or whitespace-padded
` true ` do not become newly enabled overwrite requests. No public schema is
mutated. Typed JSON files and other SDK environment conversions are unchanged.

The regression suite uses the real CLI, initialization and EOF path, observes the
resolved config, and guards external client factories. It isolates dotenv lookup
and environment state. Coverage includes all three selectors, CSV/JSON arrays,
exclusions, boolean compatibility, a dotenv profile, subclass prefixes, file/ENV
precedence in both argument orders, nested defaults, object replacement, and
malformed nested JSON failing before initialization.

## Primary sources checked 2026-11-28

- [Meltano configuration](https://docs.meltano.com/guide/configuration/): Meltano generates plugin JSON and casts settings according to metadata. Inspect the installed definition with `meltano config list` and the resolved JSON with `meltano config print`.
- [Singer SDK CLI](https://sdk.meltano.com/en/latest/implementation/cli.html#config): direct ENV parsing, scalar/object types and environment-over-file precedence.
- [Installed SDK 0.54.5 parser](https://github.com/meltano/sdk/blob/v0.54.5/singer_sdk/configuration/_dict_config.py): boolean conversion precedes array/object parsing.

No Meltano installation or Hub metadata was changed. The typed JSON fixture models
Meltano's documented output; a full Meltano-managed invocation was not run.
Cloud ingestion, credentials and permissions were neither used nor tested.
