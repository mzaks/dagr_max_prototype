"""Dagr schemas for the MAX serve batch-log prototype.

- MaxBatchLog      DataSink: one packed BatchStep record per scheduler step (the log file).
- BatchStepSlot    SharedBuffer: the same fields as a fixed-layout record that Python fills
                   and Mojo reads in place before appending it to the sink.

Build: .venv/bin/python -I "$(which dagr)"  -- or simply: dagr build
"""
import os
import sys

try:                                    # the copy `dagr build` writes into gen/python, whose
    from dagr_config import Library, Mojo, Python          # runtime ships the DSL flat; the
    from dagr_dsl import DataSink, Enum, Node, SharedBuffer, required, t   # classes must be the
except ModuleNotFoundError:             # same objects the generated layout engine sees
    from dagr.config import Library, Mojo, Python           # the CLI's own package layout
    from dagr.dsl import DataSink, Enum, Node, SharedBuffer, required, t

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from record_spec import EXTRA, FIELDS, SUBRECORDS  # noqa: E402

BATCH_TYPE = Enum("BatchType", ["CE", "TG"])

MAX_BATCH_LOG = DataSink(
    "MaxBatchLog",
    node_types=[
        Node("BatchStep", fields=[                              # typeId 0
            "ts_ns"                  >> t.u64 >> required,
            "batch_type"             >> t.ref("BatchType") >> required,
            "batch_size"             >> t.u32 >> required,
            "terminated_reqs"        >> t.u32,
            "pending_reqs"           >> t.u32,
            "input_tokens"           >> t.u32,
            "context_tokens"         >> t.u64,
            "creation_time_s"        >> t.f64 >> required,
            "execution_time_s"       >> t.f64,
            "prompt_throughput"      >> t.f64,
            "generation_throughput"  >> t.f64,
            "preemptions_total"      >> t.u64,
            "used_kv_pct"            >> t.f64,
            "total_kv_blocks"        >> t.u32,
            "cache_hit_tokens"       >> t.u32,
            "cache_miss_tokens"      >> t.u32,
            "draft_tokens_generated" >> t.u32,
            "draft_tokens_accepted"  >> t.u32,
            "overlap_active"         >> t.bool,
            "completed"              >> t.ref("CompletedBatch"),
        ]),
        BATCH_TYPE,                                             # typeId 1
        Node("CompletedBatch", fields=[                         # typeId 2
            "batch_type"       >> t.ref("BatchType") >> required,
            "batch_size"       >> t.u32 >> required,
            "input_tokens"     >> t.u32,
            "context_tokens"   >> t.u64,
            "execution_time_s" >> t.f64 >> required,
        ]),
    ],
    header=Node("BatchLogHeader", fields=[
        "producer"       >> t.utf8 >> required,
        "model"          >> t.utf8 >> required,
        "max_batch_size" >> t.u32 >> required,
    ]),
    doubly_linked=True,
)

# Same fields, fixed layout. Optional fields -> presence bits; `completed` is an optional
# inline child. Python writes one record in place; Mojo reads it and appends to the sink.
BATCH_STEP_SLOT = SharedBuffer(
    "BatchStepSlot",
    root_type="BatchStepRec",
    concurrency="none",
    node_types=[
        Node("BatchStepRec", frozen=True, fields=[
            "ts_ns"                  >> t.u64 >> required,
            "batch_type"             >> t.ref("SlotBatchType") >> required,
            "batch_size"             >> t.u32 >> required,
            "terminated_reqs"        >> t.u32,
            "pending_reqs"           >> t.u32,
            "input_tokens"           >> t.u32,
            "context_tokens"         >> t.u64,
            "creation_time_s"        >> t.f64 >> required,
            "execution_time_s"       >> t.f64,
            "prompt_throughput"      >> t.f64,
            "generation_throughput"  >> t.f64,
            "preemptions_total"      >> t.u64,
            "used_kv_pct"            >> t.f64,
            "total_kv_blocks"        >> t.u32,
            "cache_hit_tokens"       >> t.u32,
            "cache_miss_tokens"      >> t.u32,
            "draft_tokens_generated" >> t.u32,
            "draft_tokens_accepted"  >> t.u32,
            "overlap_active"         >> t.bool,
            "completed"              >> t.ref("CompletedRec"),
        ]),
        Enum("SlotBatchType", ["CE", "TG"]),
        Node("CompletedRec", frozen=True, fields=[
            "batch_type"       >> t.ref("SlotBatchType") >> required,
            "batch_size"       >> t.u32 >> required,
            "input_tokens"     >> t.u32,
            "context_tokens"   >> t.u64,
            "execution_time_s" >> t.f64 >> required,
        ]),
    ],
)


# Design 1: the full per-step BatchMetrics record (see record_spec.py). The worker appends
# one BatchMetricsRec per scheduler step; the telemetry process publishes from the stream.
def _field(name, kind, enum_ref):
    if kind == "u64":
        return name >> t.u64 >> required
    if kind == "f64":
        return name >> t.f64 >> required
    if kind == "bool":
        return name >> t.bool >> required
    if kind == "batch_type":
        return name >> t.ref(enum_ref) >> required
    if kind == "opt_f64":
        return name >> t.f64
    if kind == "opt_u64":
        return name >> t.u64
    if kind == "f64_list":
        return name >> t.f64.array
    if kind == "u64_list":
        return name >> t.u64.array
    for sub_kind, node_name, _ in SUBRECORDS:
        if kind == sub_kind:
            return name >> t.ref(node_name)
    raise ValueError(kind)


MAX_BATCH_METRICS = DataSink(
    "MaxBatchMetrics",
    node_types=[
        Node("BatchMetricsRec", fields=[                       # typeId 0
            "ts_ns" >> t.u64 >> required,
            *[_field(n, k, "MetricsBatchType") for n, k in FIELDS],
            *[_field(n, k, "MetricsBatchType") for n, k in EXTRA],
        ]),
        Enum("MetricsBatchType", ["CE", "TG"]),                # typeId 1
        *[Node(node_name, fields=[                             # typeId 2, 3, 4
            *[_field(n, k, "MetricsBatchType") for n, k in sub_fields],
        ]) for _, node_name, sub_fields in SUBRECORDS],
    ],
    header=Node("MetricsLogHeader", fields=[
        "producer" >> t.utf8 >> required,
        "model"    >> t.utf8 >> required,
    ]),
    doubly_linked=True,
)

# Generic MetricClient measurements (API process + worker), replacing the pickled
# multiprocessing.Queue. Instrument names and attribute sets are interned: a NameRec /
# AttrSetRec is written the first time a producer uses one, and MeasurementRec refers to them
# by id, so a measurement costs ts + two ids + a value.
MAX_MEASUREMENTS = DataSink(
    "MaxMeasurements",
    node_types=[
        Node("MeasurementRec", fields=[                        # typeId 0
            "ts_ns"    >> t.u64 >> required,
            "name_id"  >> t.u32 >> required,
            "attrs_id" >> t.u32 >> required,
            "value"    >> t.f64 >> required,
        ]),
        Node("NameRec", fields=[                               # typeId 1
            "id"   >> t.u32 >> required,
            "name" >> t.utf8 >> required,
        ]),
        Node("AttrSetRec", fields=[                            # typeId 2
            "id" >> t.u32 >> required,
            "kv" >> t.utf8.array,                              # [k0, v0, k1, v1, ...]
        ]),
    ],
    header=Node("MeasurementLogHeader", fields=[
        "producer" >> t.utf8 >> required,
        "pid"      >> t.u64 >> required,
    ]),
    doubly_linked=True,
)

library = Library(
    "max_batch_log",
    schemas=[MAX_BATCH_LOG, BATCH_STEP_SLOT, MAX_BATCH_METRICS, MAX_MEASUREMENTS],
    targets=[
        Mojo(out="gen/mojo"),
        Python(out="gen/python"),   # reflective reader (independent check) + SharedBuffer layout
    ],
)
