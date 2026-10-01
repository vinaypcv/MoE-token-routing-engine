use moe_holistic_engine::engine::elastic_quant::ElasticQuantizer;
use std::hint::black_box;
use std::time::{Duration, Instant};

const SAMPLES: usize = 10_000;
const FEATURES: usize = 256;

fn percentile(mut samples: Vec<Duration>, rank: f64) -> Duration {
    samples.sort_unstable();
    let index = ((samples.len() as f64 - 1.0) * rank).round() as usize;
    samples[index]
}

fn main() {
    let features: Vec<f32> = (0..FEATURES)
        .map(|index| (index as f32 - 128.0) / 64.0)
        .collect();
    let quantizer = ElasticQuantizer::new(90, Some(7)).expect("valid quantizer");
    let mut fp32_latencies = Vec::with_capacity(SAMPLES);
    let mut int8_latencies = Vec::with_capacity(SAMPLES);
    let fp32_started = Instant::now();
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let sum: f32 = features.iter().copied().sum();
        black_box(sum);
        fp32_latencies.push(started.elapsed());
    }
    let fp32_elapsed = fp32_started.elapsed();

    let int8_started = Instant::now();
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let quantized = quantizer
            .quantize_fp32(&features)
            .expect("features are non-empty");
        black_box(quantized);
        int8_latencies.push(started.elapsed());
    }
    let int8_elapsed = int8_started.elapsed();

    println!("=== QUANTIZATION PROFILE BENCHMARK ===");
    println!("Samples                       : {SAMPLES}");
    println!("Features per sample          : {FEATURES}");
    println!(
        "FP32 throughput (samples/s)  : {:.2}",
        SAMPLES as f64 / fp32_elapsed.as_secs_f64()
    );
    println!(
        "INT8 throughput (samples/s)  : {:.2}",
        SAMPLES as f64 / int8_elapsed.as_secs_f64()
    );
    println!(
        "FP32 p99 latency             : {:?}",
        percentile(fp32_latencies, 0.99)
    );
    println!(
        "INT8 p99 latency             : {:?}",
        percentile(int8_latencies, 0.99)
    );
    println!("High-watermark policy        : queue depth >= 90% selects fallback/quantize");
    println!("=========================================");
}
