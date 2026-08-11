# Test utilities module guidance

Read `.agents/references/modules/flink-connector-gcp-test-utils.md` and ADR-0050/ADR-0051 before
changing this module. Keep it test-support-only with provided dependencies; shared production code
belongs in `flink-connector-gcp-base`. Do not force unrelated emulator fixtures into one hierarchy,
and preserve the justfile install-list coupling for reactor-sibling consumers.
