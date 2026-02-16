use super::*;

#[path = "transfer_reader_codec.rs"]
mod codec;
#[path = "transfer_reader_write.rs"]
mod write_ops;

pub use codec::{
    build_transfer_columns, convert_boolean_to_f64, convert_date_to_stata_days,
    convert_datetime_to_stata_clock, convert_f32_to_f64, convert_f64_to_f64, convert_i16_to_f64,
    convert_i32_to_f64, convert_i64_to_f64, convert_i8_to_f64, convert_strict_string,
    convert_time_to_stata_millis, convert_u16_to_f64, convert_u32_to_f64, convert_u64_to_f64,
    convert_u8_to_f64, dtype_mismatch_error, estimate_transfer_row_width_bytes, CellConversion,
    TransferColumnSpec,
};
pub use write_ops::{write_numeric_column_range, write_string_column_range};

pub fn sink_dataframe_in_batches(
    df: &DataFrame,
    start_index_base: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
    batch_tuner: &mut AdaptiveBatchTuner,
    processed_batches: &mut usize,
) -> PolarsResult<(usize, usize)> {
    let total_rows = df.height();
    let mut loaded_rows = 0usize;
    let mut n_batches = 0usize;
    let mut batch_offset = 0usize;

    while batch_offset < total_rows {
        let batch_length = (total_rows - batch_offset).min(batch_tuner.selected_batch_size());
        let batch_df = df.slice(batch_offset as i64, batch_length);
        if batch_df.height() == 0 {
            break;
        }

        let batch_started_at = Instant::now();
        process_batch_with_strategy(
            &batch_df,
            start_index_base + batch_offset,
            transfer_columns,
            strategy,
            stata_offset,
        )?;

        let batch_rows = batch_df.height();
        loaded_rows += batch_rows;
        *processed_batches += 1;
        n_batches += 1;
        batch_offset += batch_rows;
        batch_tuner.observe_batch(batch_rows, batch_started_at.elapsed().as_millis());
    }

    Ok((loaded_rows, n_batches))
}

fn process_batch_with_strategy(
    batch: &DataFrame,
    start_index: usize,
    transfer_columns: &[TransferColumnSpec],
    strategy: BatchMode,
    stata_offset: usize,
) -> PolarsResult<()> {
    let row_count = batch.height();
    let pool = get_compute_thread_pool();
    if pool.current_num_threads() <= 1 || row_count < 4_096 {
        return process_row_range(
            batch,
            start_index,
            0,
            row_count,
            transfer_columns,
            stata_offset,
        );
    }

    pool.install(|| match strategy {
        BatchMode::ByRow => {
            let n_workers = rayon::current_num_threads().max(1);
            let chunk_size = std::cmp::max(512, row_count.div_ceil(n_workers * 8));
            (0..row_count)
                .into_par_iter()
                .chunks(chunk_size)
                .try_for_each(|chunk| {
                    let start_row = chunk[0];
                    let end_row = chunk[chunk.len() - 1] + 1;
                    process_row_range(
                        batch,
                        start_index,
                        start_row,
                        end_row,
                        transfer_columns,
                        stata_offset,
                    )
                })
        }
        BatchMode::ByColumn => transfer_columns.par_iter().try_for_each(|transfer_column| {
            let col = batch.column(&transfer_column.name)?;
            write_ops::write_transfer_column_range(
                col,
                transfer_column,
                start_index,
                0,
                row_count,
                stata_offset,
            )
        }),
    })
}

fn process_row_range(
    batch: &DataFrame,
    start_index: usize,
    start_row: usize,
    end_row: usize,
    transfer_columns: &[TransferColumnSpec],
    stata_offset: usize,
) -> PolarsResult<()> {
    for transfer_column in transfer_columns {
        let col = batch.column(&transfer_column.name)?;
        write_ops::write_transfer_column_range(
            col,
            transfer_column,
            start_index,
            start_row,
            end_row,
            stata_offset,
        )?;
    }
    Ok(())
}
