#!/usr/bin/env python3
"""Turn a run's raw output.log into the base-result CSVs.

Parses the metric tags emitted by holon.examples.nexmark.consumers.LagAppendOutputConsumer
(and the producer/node rate logs) into six CSVs. The same consumer/logback config is used by
both the Holon and Flink deployments, so this script is platform-agnostic: tags a platform
doesn't emit simply yield header-only CSVs.

Invoked automatically by experiment/08-export.sh after each run:
  python parse_output_log.py --input <output.log> --out-dir <dir> --base-name <LABEL>

The CSVs are the raw computed results (latency, throughput, state size, message size, producer
rate, process rate) used downstream for plotting / further analysis.
"""
import argparse
import csv
import os
import re
import sys


def process_file(base_name: str, file_path: str, base_path: str) -> dict:
    # Containers for the various metrics
    input_timestamps      = {}  # window_id -> max input timestamp
    output_timestamps     = {}  # window_id -> min output timestamp
    throughput_counts     = {}  # window_id -> sum of eventCounts
    state_timestamps      = {}  # state_timestamp -> (partition, state_size)
    message_timestamps    = {}  # (timestamp, node_id) -> (bytes_out, bytes_in)
    producer_rate_records = []  # list of dicts: {timestamp, producer, count, duration_ms, second}
    process_rate_records  = []  # list of dicts: {timestamp, partition, count, duration_ms, second}

    # Compile regexes for each metric tag
    pattern_input = re.compile(r'\[LagAppendInput\].*window:\s*(\d+),\s*timestamp:\s*(\d+)')
    pattern_output = re.compile(r'\[LagAppendOutput\].*window:\s*(\d+),\s*timestamp:\s*(\d+)')
    pattern_throughput = re.compile(r'\[Throughput\]\s*window:\s*(\d+),\s*eventCount:\s*(\d+)')
    pattern_state = re.compile(
        r'\[STATE-SIZE\]:\s*partition:\s*(\d+),\s*size:\s*(\d+),\s*timestamp:\s*(\d+)'
    )
    pattern_message = re.compile(
        r'\[MESSAGING-SIZE\]\s*timestamp:\s*(\d+),\s*'    # capture group 1 = ts
        r'(\d+)\s*Bytes OUT:\s*([\d,]+),\s*Bytes IN:\s*([\d,]+)'
        #                   group 2 = node_id
        #                   group 3 = bytes_out (with commas)
        #                   group 4 = bytes_in (with commas)
    )

    # ProducerRate (capturing 'second' at end)
    pattern_producer = re.compile(
        r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*'
        r'\[ProducerRate\]\s*producer\s*(?P<producer>\d+)\s*produced\s*'
        r'(?P<count>\d+)\s*events\s*in\s*last\s*(?P<duration>\d+)ms.*,\s*second:\s*(?P<second>\d+)'
    )

    # ProcessRate (capturing partition + second)
    pattern_process = re.compile(
        r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*'
        r'\[ProcessRate\]\s*partition\s*(?P<partition>\d+)\s*processed\s*'
        r'(?P<count>\d+)\s*events\s*in\s*last\s*(?P<duration>\d+)ms.*,\s*second:\s*(?P<second>\d+)'
    )

    # Read and parse each line
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()

            # LagAppendInput
            m = pattern_input.search(line)
            if m:
                win, ts = int(m.group(1)), int(m.group(2))
                input_timestamps[win] = max(input_timestamps.get(win, 0), ts)
                continue

            # LagAppendOutput
            m = pattern_output.search(line)
            if m:
                win, ts = int(m.group(1)), int(m.group(2))
                current = output_timestamps.get(win)
                output_timestamps[win] = ts if (current is None or ts < current) else current
                continue

            # Throughput
            m = pattern_throughput.search(line)
            if m:
                win, count = int(m.group(1)), int(m.group(2))
                throughput_counts[win] = throughput_counts.get(win, 0) + count
                continue

            # STATE-SIZE
            m = pattern_state.search(line)
            if m:
                partition = int(m.group(1))
                size      = int(m.group(2))
                ts        = int(m.group(3))
                state_timestamps[ts] = (partition, size)
                continue

            # MESSAGING-SIZE
            m = pattern_message.search(line)
            if m:
                ts        = int(m.group(1))
                node_id   = int(m.group(2))
                # Remove commas before parsing to int
                bytes_out = int(m.group(3).replace(',', ''))
                bytes_in  = int(m.group(4).replace(',', ''))
                message_timestamps[(ts, node_id)] = (bytes_out, bytes_in)
                continue

            # ProducerRate
            m = pattern_producer.search(line)
            if m:
                producer_rate_records.append({
                    'timestamp': m.group('ts'),
                    'producer_index': int(m.group('producer')),
                    'event_count': int(m.group('count')),
                    'duration_ms': int(m.group('duration')),
                    'second': int(m.group('second')),
                })
                continue

            # ProcessRate
            m = pattern_process.search(line)
            if m:
                process_rate_records.append({
                    'timestamp': m.group('ts'),
                    'partition': int(m.group('partition')),
                    'event_count': int(m.group('count')),
                    'duration_ms': int(m.group('duration')),
                    'second': int(m.group('second')),
                })
                continue

    # Write output files into out-dir, prefixed by base_name.
    out_dir = base_path
    os.makedirs(out_dir, exist_ok=True)

    latency_file        = os.path.join(out_dir, f"{base_name}_latency.csv")
    throughput_file     = os.path.join(out_dir, f"{base_name}_throughput.csv")
    state_file          = os.path.join(out_dir, f"{base_name}_state_size.csv")
    message_file        = os.path.join(out_dir, f"{base_name}_message_size.csv")
    producer_rate_file  = os.path.join(out_dir, f"{base_name}_producer_rate.csv")
    process_rate_file   = os.path.join(out_dir, f"{base_name}_process_rate.csv")

    # 1) Latency CSV
    common = set(input_timestamps) & set(output_timestamps)
    with open(latency_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["output_timestamp", "latency"])
        for win in sorted(common):
            out_ts = output_timestamps[win]
            writer.writerow([out_ts, out_ts - input_timestamps[win]])

    # 2) Throughput CSV
    with open(throughput_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["output_timestamp", "record_count"])
        for win, out_ts in sorted(output_timestamps.items(), key=lambda kv: kv[1]):
            writer.writerow([out_ts, throughput_counts.get(win, 0)])

    # 3) State size CSV
    with open(state_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "partition", "state_size"])
        for ts in sorted(state_timestamps):
            part, size = state_timestamps[ts]
            writer.writerow([ts, part, size])

    # 4) Message size CSV
    with open(message_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "node_id", "bytes_out", "bytes_in"])
        for (ts, node), (bout, bin_) in sorted(message_timestamps.items()):
            writer.writerow([ts, node, bout, bin_])

    # 5) Producer rate CSV
    with open(producer_rate_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "producer_index", "event_count", "duration_ms", "second"])
        for rec in producer_rate_records:
            writer.writerow([rec['timestamp'], rec['producer_index'], rec['event_count'],
                             rec['duration_ms'], rec['second']])

    # 6) Process rate CSV
    with open(process_rate_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "partition", "event_count", "duration_ms", "second"])
        for rec in process_rate_records:
            writer.writerow([rec['timestamp'], rec['partition'], rec['event_count'],
                             rec['duration_ms'], rec['second']])

    # Row counts (header excluded) — printed so the runner can fold them into summary.txt.
    return {
        "latency": len(common),
        "throughput": len(output_timestamps),
        "state_size": len(state_timestamps),
        "message_size": len(message_timestamps),
        "producer_rate": len(producer_rate_records),
        "process_rate": len(process_rate_records),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse a run's output.log into base-result CSVs.")
    ap.add_argument("--input", required=True, help="Path to the raw output.log")
    ap.add_argument("--out-dir", required=True, help="Directory to write the CSVs into")
    ap.add_argument("--base-name", required=True, help="Filename prefix for the CSVs (e.g. the run label)")
    args = ap.parse_args()

    if not os.path.isfile(args.input):
        print(f"ERROR: input log not found: {args.input}", file=sys.stderr)
        return 1
    if os.path.getsize(args.input) == 0:
        print(f"ERROR: input log is empty: {args.input}", file=sys.stderr)
        return 1

    counts = process_file(args.base_name, args.input, args.out_dir)

    print(f"Parsed {args.input} -> {args.out_dir} (prefix {args.base_name})")
    for name in ("latency", "throughput", "state_size", "message_size",
                 "producer_rate", "process_rate"):
        print(f"  {name}: {counts[name]} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
