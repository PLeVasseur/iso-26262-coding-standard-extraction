use super::*;

pub fn stage_metric_check(
    check_id: &str,
    name: &str,
    stage: Wp2GateStage,
    metric: Option<f64>,
    hard_fail: bool,
    stage_b_fail: bool,
) -> QualityCheck {
    let result = if metric.is_none() {
        "pending".to_string()
    } else {
        wp2_result(stage, hard_fail, stage_b_fail).to_string()
    };

    QualityCheck {
        check_id: check_id.to_string(),
        name: name.to_string(),
        result,
    }
}
