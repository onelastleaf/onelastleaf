#![feature(random)]

//! Run with `cargo run --release --example hashmap_load_factor`.
//! Each configuration fills one random table until its first insertion error.

use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL_CONDENSED};
use onelastleaf::storage::hashmap::{HashMap, InsertError};
use std::hint::black_box;
use std::random::random;
use std::time::{Duration, Instant};

const SLOTS_PER_BUCKET: usize = 8;
const SUBTABLE_COUNT: usize = 256;
const CANDIDATE_COUNT: usize = 3;
const BUCKET_COUNTS: [usize; 6] = [1, 2, 4, 8, 16, 32];
const CHECKPOINT_PERCENTAGES: [usize; 5] = [50, 75, 90, 95, 98];
const SUCCESSFUL_LOOKUPS_PER_GROUP: usize = 10;
const LOOKUP_GROUP_COUNT: usize = 10_000;

type LoadTestMap = HashMap<u128, u64, SLOTS_PER_BUCKET, SUBTABLE_COUNT, CANDIDATE_COUNT>;

struct InsertStatistics {
    successful_inserts: usize,
    throughput_millions_per_second: f64,
    percentile_50_nanoseconds: u128,
    percentile_95_nanoseconds: u128,
    percentile_99_nanoseconds: u128,
    percentile_999_nanoseconds: u128,
    maximum_nanoseconds: u128,
}

struct CheckpointResult {
    insert_statistics: InsertStatistics,
    lookup_throughput_millions_per_second: f64,
}

struct LoadTestResult {
    buckets_per_subtable: usize,
    capacity: usize,
    successful_inserts: usize,
    checkpoint_results: [Option<CheckpointResult>; CHECKPOINT_PERCENTAGES.len()],
    final_segment_statistics: Option<InsertStatistics>,
    first_error: InsertError<u128, u64>,
    failed_insert_latency: Duration,
}

fn summarize_insert_latencies(insert_latencies: &[Duration]) -> InsertStatistics {
    let total_insert_time: Duration = insert_latencies.iter().sum();
    let mut sorted_latencies = insert_latencies.to_vec();
    sorted_latencies.sort_unstable();

    // Nearest-rank percentiles: a one-sample segment has the same value throughout.
    let percentile_nanoseconds = |permille: usize| {
        let sample_index = (sorted_latencies.len() * permille).div_ceil(1000) - 1;
        sorted_latencies[sample_index].as_nanos()
    };

    InsertStatistics {
        successful_inserts: insert_latencies.len(),
        throughput_millions_per_second: insert_latencies.len() as f64
            / total_insert_time.as_secs_f64()
            / 1_000_000.0,
        percentile_50_nanoseconds: percentile_nanoseconds(500),
        percentile_95_nanoseconds: percentile_nanoseconds(950),
        percentile_99_nanoseconds: percentile_nanoseconds(990),
        percentile_999_nanoseconds: percentile_nanoseconds(999),
        maximum_nanoseconds: sorted_latencies.last().unwrap().as_nanos(),
    }
}

fn random_inserted_entry_index(inserted_entry_count: usize) -> usize {
    // Sample uniformly within the next power of two, then reject out-of-range indices.
    let index_mask = inserted_entry_count.next_power_of_two() - 1;
    loop {
        let random_bits: usize = random(..);
        let candidate_index = random_bits & index_mask;
        if candidate_index < inserted_entry_count {
            return candidate_index;
        }
    }
}

fn measure_lookup_throughput(map: &LoadTestMap, inserted_entries: &[(u128, u64)]) -> f64 {
    let lookups_per_group = SUCCESSFUL_LOOKUPS_PER_GROUP + 1;
    let mut lookup_keys = Vec::with_capacity(LOOKUP_GROUP_COUNT * lookups_per_group);

    for _ in 0..LOOKUP_GROUP_COUNT {
        for _ in 0..SUCCESSFUL_LOOKUPS_PER_GROUP {
            let inserted_entry_index = random_inserted_entry_index(inserted_entries.len());
            lookup_keys.push(inserted_entries[inserted_entry_index].0);
        }
        // Confirm absence outside the timed region, including the unlikely collision case.
        let missing_key = loop {
            let candidate_key = random(..);
            if map.lookup(&candidate_key).is_none() {
                break candidate_key;
            }
        };
        lookup_keys.push(missing_key);
    }

    // Validate the requested hit/miss ratio and warm up this checkpoint's queries.
    for (lookup_index, lookup_key) in lookup_keys.iter().enumerate() {
        let expected_hit = lookup_index % lookups_per_group < SUCCESSFUL_LOOKUPS_PER_GROUP;
        assert_eq!(map.lookup(lookup_key).is_some(), expected_hit);
    }

    let lookup_start = Instant::now();
    for lookup_key in &lookup_keys {
        black_box(map.lookup(black_box(lookup_key)));
    }
    let lookup_elapsed = lookup_start.elapsed();

    lookup_keys.len() as f64 / lookup_elapsed.as_secs_f64() / 1_000_000.0
}

fn run_load_test(buckets_per_subtable: usize) -> LoadTestResult {
    let capacity = SLOTS_PER_BUCKET * SUBTABLE_COUNT * buckets_per_subtable;
    let mut map = LoadTestMap::new(buckets_per_subtable);
    // One extra entry guarantees an insertion error even if every slot can be filled.
    // Generation and allocation are outside the timed insertion calls.
    let generated_entries: Vec<(u128, u64)> =
        (0..=capacity).map(|_| (random(..), random(..))).collect();
    let mut successful_insert_latencies = Vec::with_capacity(capacity);
    let mut checkpoint_results = std::array::from_fn(|_| None);
    let mut next_checkpoint_index = 0;
    let mut segment_start_index = 0;

    let (first_error, failed_insert_latency) = loop {
        let (inserted_key, inserted_value) = generated_entries[successful_insert_latencies.len()];
        let insert_start = Instant::now();
        let insert_result = map.insert(black_box(inserted_key), black_box(inserted_value));
        let insert_latency = insert_start.elapsed();

        match insert_result {
            Ok(()) => successful_insert_latencies.push(insert_latency),
            Err(insert_error) => break (insert_error, insert_latency),
        }

        let successful_inserts = successful_insert_latencies.len();
        if next_checkpoint_index < CHECKPOINT_PERCENTAGES.len()
            && successful_inserts
                == (capacity * CHECKPOINT_PERCENTAGES[next_checkpoint_index]).div_ceil(100)
        {
            let insert_statistics = summarize_insert_latencies(
                &successful_insert_latencies[segment_start_index..successful_inserts],
            );
            let lookup_throughput_millions_per_second =
                measure_lookup_throughput(&map, &generated_entries[..successful_inserts]);
            checkpoint_results[next_checkpoint_index] = Some(CheckpointResult {
                insert_statistics,
                lookup_throughput_millions_per_second,
            });
            next_checkpoint_index += 1;
            segment_start_index = successful_inserts;
        }
    };

    let successful_inserts = successful_insert_latencies.len();
    let final_segment_statistics = (segment_start_index < successful_inserts)
        .then(|| summarize_insert_latencies(&successful_insert_latencies[segment_start_index..]));

    // Check that displacement preserved every value; this does not affect the timings.
    for (inserted_key, inserted_value) in &generated_entries[..successful_inserts] {
        assert_eq!(map.lookup(inserted_key), Some(inserted_value));
    }

    LoadTestResult {
        buckets_per_subtable,
        capacity,
        successful_inserts,
        checkpoint_results,
        final_segment_statistics,
        first_error,
        failed_insert_latency,
    }
}

fn error_name(insert_error: &InsertError<u128, u64>) -> &'static str {
    match insert_error {
        InsertError::DuplicateKey(..) => "DuplicateKey",
        InsertError::NoSpace(..) => "NoSpace",
        InsertError::FalseEmpty(..) => "FalseEmpty",
        InsertError::FalseOccupied(..) => "FalseOccupied",
        InsertError::BfsTreeError(..) => "BfsTreeError",
        InsertError::RelocationError(..) => "RelocationError",
    }
}

fn print_summary(results: &[LoadTestResult]) {
    let mut summary_table = Table::new();
    summary_table
        .load_style(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "Buckets /\nsubtable",
            "Capacity",
            "Inserted",
            "Load at first\nerror (%)",
            "First error",
            "Failed insert\n(us)",
        ]);

    for result in results {
        summary_table.add_row([
            result.buckets_per_subtable.to_string(),
            result.capacity.to_string(),
            result.successful_inserts.to_string(),
            format!(
                "{:.2}",
                result.successful_inserts as f64 / result.capacity as f64 * 100.0
            ),
            error_name(&result.first_error).to_owned(),
            format!(
                "{:.2}",
                result.failed_insert_latency.as_secs_f64() * 1_000_000.0
            ),
        ]);
    }

    println!("\nLoad at first insertion error (one random table per configuration)");
    println!("{summary_table}");
}

fn measurement_cells(
    insert_statistics: Option<&InsertStatistics>,
    lookup_throughput_millions_per_second: Option<f64>,
) -> Vec<String> {
    let Some(insert_statistics) = insert_statistics else {
        return vec![String::new(); 8];
    };

    vec![
        insert_statistics.successful_inserts.to_string(),
        format!("{:.3}", insert_statistics.throughput_millions_per_second),
        lookup_throughput_millions_per_second
            .map(|throughput| format!("{throughput:.3}"))
            .unwrap_or_default(),
        insert_statistics.percentile_50_nanoseconds.to_string(),
        insert_statistics.percentile_95_nanoseconds.to_string(),
        insert_statistics.percentile_99_nanoseconds.to_string(),
        insert_statistics.percentile_999_nanoseconds.to_string(),
        insert_statistics.maximum_nanoseconds.to_string(),
    ]
}

fn print_checkpoint_results(results: &[LoadTestResult]) {
    let mut checkpoint_table = Table::new();
    checkpoint_table
        .load_style(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            "Buckets /\nsubtable",
            "Load interval",
            "Inserts",
            "Insert\nMops/s",
            "Lookup\nMops/s",
            "p50\n(ns)",
            "p95\n(ns)",
            "p99\n(ns)",
            "p99.9\n(ns)",
            "Max\n(ns)",
        ]);

    for result in results {
        let mut interval_start_percentage = 0;
        let mut last_reached_percentage = 0;
        for (checkpoint_percentage, checkpoint_result) in CHECKPOINT_PERCENTAGES
            .iter()
            .zip(&result.checkpoint_results)
        {
            let mut checkpoint_row = vec![
                result.buckets_per_subtable.to_string(),
                format!("{interval_start_percentage}-{checkpoint_percentage}%"),
            ];
            checkpoint_row.extend(measurement_cells(
                checkpoint_result
                    .as_ref()
                    .map(|checkpoint| &checkpoint.insert_statistics),
                checkpoint_result
                    .as_ref()
                    .map(|checkpoint| checkpoint.lookup_throughput_millions_per_second),
            ));
            checkpoint_table.add_row(checkpoint_row);
            interval_start_percentage = *checkpoint_percentage;
            if checkpoint_result.is_some() {
                last_reached_percentage = *checkpoint_percentage;
            }
        }

        let final_load_percentage =
            result.successful_inserts as f64 / result.capacity as f64 * 100.0;
        let mut final_segment_row = vec![
            result.buckets_per_subtable.to_string(),
            format!("{last_reached_percentage}-{final_load_percentage:.2}% (stop)"),
        ];
        final_segment_row.extend(measurement_cells(
            result.final_segment_statistics.as_ref(),
            None,
        ));
        checkpoint_table.add_row(final_segment_row);
    }

    println!("\nInsert statistics by interval; lookup throughput at the interval's checkpoint");
    println!("{checkpoint_table}");
}

fn main() {
    println!("K=u128, V=u64, B=8, S=256, H=3; resize is never called.");
    println!(
        "Each checkpoint runs 100,000 successful and 10,000 unsuccessful lookups after warmup."
    );
    println!("Checkpoints round up to whole slots; unreached checkpoint measurements stay blank.");
    println!(
        "Insert throughput = successful calls / sum of measured call times (timer overhead included)."
    );
    println!("Random generation, lookup preparation and statistics are outside insertion timings.");
    println!("Insert percentiles cover successful calls; failed-call time includes error logging.");

    let results: Vec<_> = BUCKET_COUNTS.into_iter().map(run_load_test).collect();
    print_summary(&results);
    print_checkpoint_results(&results);
}
