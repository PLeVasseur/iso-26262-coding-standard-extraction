fn is_first_hit_intent(intent: &str) -> bool {
    let normalized = intent.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "exact_ref" | "keyword" | "table_intent"
    )
}

fn sign_test_two_sided_p_value(deltas: &[f64]) -> Option<f64> {
    let wins = deltas.iter().filter(|delta| **delta > 0.0).count();
    let losses = deltas.iter().filter(|delta| **delta < 0.0).count();
    let n = wins + losses;
    if n == 0 {
        return None;
    }

    let k = wins.min(losses);
    let mut tail = 0.0_f64;
    for i in 0..=k {
        tail += binomial_pmf_half(n, i);
    }
    Some((2.0 * tail).min(1.0))
}

fn binomial_pmf_half(n: usize, k: usize) -> f64 {
    if k > n {
        return 0.0;
    }

    let mut coefficient = 1.0_f64;
    let top_k = k.min(n - k);
    for i in 0..top_k {
        coefficient *= (n - i) as f64 / (i + 1) as f64;
    }
    coefficient * 0.5_f64.powi(n as i32)
}

fn bootstrap_confidence_interval_95(
    deltas: &[f64],
    iterations: usize,
    seed: u64,
) -> Option<(Option<f64>, Option<f64>)> {
    if deltas.is_empty() || iterations == 0 {
        return None;
    }

    let mut rng = seed;
    let mut means = Vec::<f64>::with_capacity(iterations);
    for _ in 0..iterations {
        let mut total = 0.0_f64;
        for _ in 0..deltas.len() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let index = (rng as usize) % deltas.len();
            total += deltas[index];
        }
        means.push(total / deltas.len() as f64);
    }

    means.sort_by(|left, right| left.total_cmp(right));
    let low_index = ((iterations as f64) * 0.025).floor() as usize;
    let high_index = ((iterations as f64) * 0.975).ceil() as usize;
    let low = means
        .get(low_index.min(iterations.saturating_sub(1)))
        .copied();
    let high = means
        .get(
            high_index
                .saturating_sub(1)
                .min(iterations.saturating_sub(1)),
        )
        .copied();
    Some((low, high))
}
