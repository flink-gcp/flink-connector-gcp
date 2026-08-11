# Base module guidance

Read `.agents/references/modules/flink-connector-gcp-base.md` and its linked ADRs before changing
this module. Keep it limited to shared production primitives with compile-scope consumers; test
support belongs in `flink-connector-gcp-test-utils`. Preserve the failure, metric naming, bounded
source assignment, retry/RPC, options, and lifecycle contracts recorded there. A base-module
change can affect every connector and shaded SQL artifact, so verify the dependent reactor set.
