"""Single source of truth for the per-step BatchMetrics record (design 1).

The worker hands Mojo a flat tuple of values in FIELDS order (plus `published_in_worker`);
Mojo stores one `BatchMetricsRec` per step in the `MaxBatchMetrics` DataSink; the telemetry
process decodes records back into the same tuple shape and rebuilds `BatchMetrics`.

schema.py and gen_record_code.py (-> metrics_record_gen.mojo) are derived from this file.

Kinds:
  u64        non-negative int                  f64        float
  bool       bool                              batch_type BatchType enum (CE/TG)
  opt_f64    float | None                      opt_u64    int | None
  f64_list   list[float]                       u64_list   list[int]
  completed  CompletedBatchStats | None        vision     VisionEncoderMetrics | None
  video      VideoEncoderMetrics | None
"""

# BatchMetrics fields that publish_metrics / pretty_format / to_log_extra read, in the
# order of the worker's values tuple. vision_metrics / video_metrics follow as sub-records
# (EXTRA below), so no step has to be published in the worker.
FIELDS = [
    ("batch_type", "batch_type"),
    ("batch_size", "u64"),
    ("max_batch_size", "u64"),
    ("terminated_reqs", "u64"),
    ("num_pending_reqs", "u64"),
    ("num_input_tokens", "u64"),
    ("max_batch_input_tokens", "u64"),
    ("num_context_tokens", "u64"),
    ("max_batch_total_tokens", "u64"),
    ("batch_creation_time_s", "f64"),
    ("batch_execution_time_s", "f64"),
    ("prompt_throughput", "f64"),
    ("generation_throughput", "f64"),
    ("total_preemption_count", "u64"),
    ("used_kv_pct", "f64"),
    ("total_kv_blocks", "u64"),
    ("cache_hit_rate", "f64"),
    ("cache_hit_tokens", "u64"),
    ("cache_miss_tokens", "u64"),
    ("device_blocks_served", "u64"),
    ("used_host_kv_pct", "f64"),
    ("total_host_kv_bytes", "u64"),
    ("h2d_bytes_copied", "u64"),
    ("d2h_bytes_copied", "u64"),
    ("disk_bytes_read", "u64"),
    ("disk_bytes_written", "u64"),
    ("inflight_disk_ops", "u64"),
    ("used_disk_kv_pct", "f64"),
    ("total_disk_kv_bytes", "u64"),
    ("draft_tokens_generated", "u64"),
    ("draft_tokens_accepted", "u64"),
    ("avg_acceptance_length", "f64"),
    ("max_acceptance_length", "u64"),
    ("acceptance_rate_per_position", "f64_list"),
    ("nixl_read_latency_avg_ms", "f64"),
    ("nixl_write_latency_avg_ms", "f64"),
    ("rpc_acquire_latency_avg_ms", "f64"),
    ("rpc_read_latency_avg_ms", "f64"),
    ("nixl_read_gib_per_s", "f64"),
    ("nixl_write_gib_per_s", "f64"),
    ("nixl_read_latency_max_ms", "f64"),
    ("dkv_connected_clients", "u64"),
    ("dkv_total_clients", "u64"),
    ("dkv_reconnect_attempts", "u64"),
    ("dkv_read_blocks", "u64"),
    ("dkv_read_bytes", "u64"),
    ("cache_hit_external_tokens", "u64"),
    ("overlap_active", "bool"),
    ("completed", "completed"),
    ("dp_active_token_occupancy_pct", "opt_f64"),
    ("dp_context_token_occupancy_pct", "opt_f64"),
    ("dp_active_tokens", "u64"),
    ("dp_step_capacity_tokens", "u64"),
    ("cross_replica_blocks_copied", "u64"),
    ("cross_replica_bytes_copied", "u64"),
    ("per_request_prefix_coverage", "f64_list"),
    ("num_new_admissions", "u64"),
]

# CompletedBatchStats, in record order (read from the object's __dict__ in the worker).
COMPLETED_FIELDS = [
    ("batch_type", "batch_type"),
    ("batch_size", "u64"),
    ("num_input_tokens", "u64"),
    ("num_context_tokens", "u64"),
    ("execution_time_s", "f64"),
    ("num_output_tokens", "opt_u64"),
    ("draft_tokens_generated", "u64"),
    ("draft_tokens_accepted", "u64"),
    ("avg_acceptance_length", "f64"),
    ("max_acceptance_length", "u64"),
    ("acceptance_rate_per_position", "f64_list"),
]

# VisionEncoderMetrics and VideoEncoderMetrics, in record order (read from __dict__).
VISION_FIELDS = [
    ("num_images_total", "u64"),
    ("num_images_encoded", "u64"),
    ("num_images_cached", "u64"),
    ("num_patches_encoded", "u64"),
    ("num_tokens_encoded", "u64"),
]

VIDEO_FIELDS = [
    ("num_clips_total", "u64"),
    ("num_clips_encoded", "u64"),
    ("num_clips_cached", "u64"),
    ("frame_counts", "u64_list"),
    ("num_tokens_encoded", "u64"),
    ("encoding_time_ms", "f64"),
]

# Sub-records stored by reference inside BatchMetricsRec: (kind, Dagr node, fields).
SUBRECORDS = [
    ("completed", "CompletedRec", COMPLETED_FIELDS),
    ("vision", "VisionRec", VISION_FIELDS),
    ("video", "VideoRec", VIDEO_FIELDS),
]

# Appended to the values tuple after FIELDS.
# published_in_worker stays first: appending the newer fields after it keeps the Dagr
# wire change additive (a field inserted before it would change its id -> major bump).
EXTRA = [
    ("published_in_worker", "bool"),
    ("vision_metrics", "vision"),
    ("video_metrics", "video"),
]

NAMES = [n for n, _ in FIELDS]
COMPLETED_NAMES = [n for n, _ in COMPLETED_FIELDS]
VISION_NAMES = [n for n, _ in VISION_FIELDS]
VIDEO_NAMES = [n for n, _ in VIDEO_FIELDS]
