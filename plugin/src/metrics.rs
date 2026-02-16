use std::time::Instant;

use crate::utilities::{get_compute_thread_pool, get_io_thread_pool, AdaptiveBatchTuner};

pub trait RuntimeMetrics: Default {
    fn zero() -> Self;
    fn collect(&mut self, started_at: Instant);
    fn emit_to_macros(&self, prefix: &str);
}

pub trait BatchTunerMetrics {
    fn from_tuner(tuner: &AdaptiveBatchTuner) -> Self;
    fn selected_batch_size(&self) -> usize;
    fn row_width_bytes(&self) -> usize;
    fn memory_cap_rows(&self) -> usize;
    fn adjustments(&self) -> usize;
    fn tuner_mode(&self) -> &'static str;
    fn emit_to_macros(&self, prefix: &str);
}

pub struct CommonRuntimeMetrics {
    pub collect_calls: usize,
    pub planned_batches: usize,
    pub processed_batches: usize,
    pub elapsed_ms: u128,
    pub compute_pool_threads: usize,
    pub compute_pool_inits: usize,
    pub io_pool_threads: usize,
    pub io_pool_inits: usize,
}

impl Default for CommonRuntimeMetrics {
    fn default() -> Self {
        Self::zero()
    }
}

impl CommonRuntimeMetrics {
    pub fn zero() -> Self {
        Self {
            collect_calls: 0,
            planned_batches: 0,
            processed_batches: 0,
            elapsed_ms: 0,
            compute_pool_threads: get_compute_thread_pool().current_num_threads(),
            compute_pool_inits: 0,
            io_pool_threads: get_io_thread_pool().current_num_threads(),
            io_pool_inits: 0,
        }
    }
}

impl RuntimeMetrics for CommonRuntimeMetrics {
    fn zero() -> Self {
        Self::zero()
    }

    fn collect(&mut self, started_at: Instant) {
        self.elapsed_ms = started_at.elapsed().as_millis();
    }

    fn emit_to_macros(&self, prefix: &str) {
        use crate::stata_interface::{publish_transfer_metrics, set_macro};
        set_macro(
            &format!("{}_collect_calls", prefix),
            &self.collect_calls.to_string(),
            true,
        );
        set_macro(
            &format!("{}_planned_batches", prefix),
            &self.planned_batches.to_string(),
            true,
        );
        set_macro(
            &format!("{}_processed_batches", prefix),
            &self.processed_batches.to_string(),
            true,
        );
        set_macro(
            &format!("{}_elapsed_ms", prefix),
            &self.elapsed_ms.to_string(),
            true,
        );
        set_macro(
            &format!("{}_compute_pool_threads", prefix),
            &self.compute_pool_threads.to_string(),
            true,
        );
        set_macro(
            &format!("{}_compute_pool_inits", prefix),
            &self.compute_pool_inits.to_string(),
            true,
        );
        set_macro(
            &format!("{}_io_pool_threads", prefix),
            &self.io_pool_threads.to_string(),
            true,
        );
        set_macro(
            &format!("{}_io_pool_inits", prefix),
            &self.io_pool_inits.to_string(),
            true,
        );
        publish_transfer_metrics(prefix);
    }
}
