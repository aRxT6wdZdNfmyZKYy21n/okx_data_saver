"""Replay sign_only renew checkpoints for an open journal trade using trade_research NPZ."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np

from main.web_gui.trade_journal_service import get_journal_state


def _log2_to_pct(pred_log2: float) -> float:
    return (math.pow(2.0, pred_log2) - 1.0) * 100.0


def _estimate_entry_sample_index(
    entry_start_trade_id: int,
    sample_index: np.ndarray,
    entry_tids: np.ndarray,
) -> float:
    insert_index = int(np.searchsorted(entry_tids, entry_start_trade_id))
    if insert_index <= 0:
        return float(sample_index[0])
    if insert_index >= entry_tids.shape[0]:
        return float(sample_index[-1])
    tid_before = int(entry_tids[insert_index - 1])
    tid_after = int(entry_tids[insert_index])
    sample_before = float(sample_index[insert_index - 1])
    sample_after = float(sample_index[insert_index])
    if tid_after == tid_before:
        return sample_before
    fraction = (entry_start_trade_id - tid_before) / (tid_after - tid_before)
    return sample_before + fraction * (sample_after - sample_before)


def _evaluate_sign_only_checkpoint(
    side: str,
    bars_held: int,
    min_hold_steps: int,
    check_interval_steps: int,
    pred_log2: float,
    last_renew_segment_evaluated: int,
) -> tuple[bool, str, float, int]:
    pred_linear = math.pow(2.0, pred_log2) - 1.0
    current_renew_segment = bars_held // check_interval_steps
    pending_segment_eval = (
        bars_held >= min_hold_steps
        and current_renew_segment > last_renew_segment_evaluated
    )
    updated_last_renew_segment_evaluated = last_renew_segment_evaluated
    if pending_segment_eval:
        updated_last_renew_segment_evaluated = current_renew_segment
    if side == 'long':
        sign_still_valid = pred_linear > 0.0
    elif side == 'short':
        sign_still_valid = pred_linear < 0.0
    else:
        raise ValueError(f'side must be long or short, got: {side!r}')

    if bars_held < min_hold_steps:
        return False, 'before_min_hold', pred_linear, last_renew_segment_evaluated
    if not pending_segment_eval:
        return (
            False,
            'between_renew_checkpoints',
            pred_linear,
            last_renew_segment_evaluated,
        )
    if sign_still_valid:
        return (
            False,
            'sign_valid_renewed',
            pred_linear,
            updated_last_renew_segment_evaluated,
        )
    return (
        True,
        'sign_flip_at_checkpoint',
        pred_linear,
        updated_last_renew_segment_evaluated,
    )


def _nearest_row_for_target_sample(
    target_sample_index: float,
    sample_index: np.ndarray,
) -> int:
    return int(np.argmin(np.abs(sample_index - target_sample_index)))


def probe_sign_only_renew_checkpoints(
    npz_path: Path,
    entry_start_trade_id: int,
    side: str,
    min_hold_steps: int,
    check_interval_steps: int,
    max_bars: int,
) -> dict[str, object]:
    archive = np.load(npz_path, allow_pickle=True)
    sample_index = archive['sample_index']
    entry_tids = archive['entry_start_trade_id']
    pred_x32 = archive['pred_x32']

    entry_sample_index = _estimate_entry_sample_index(
        entry_start_trade_id=entry_start_trade_id,
        sample_index=sample_index,
        entry_tids=entry_tids,
    )

    checkpoint_rows: list[dict[str, object]] = []
    close_bars: list[int] = []
    last_renew_segment_evaluated = -1
    for bars_held in range(min_hold_steps, max_bars + 1, check_interval_steps):
        target_sample_index = entry_sample_index + bars_held
        row_index = _nearest_row_for_target_sample(
            target_sample_index=target_sample_index,
            sample_index=sample_index,
        )
        pred_log2 = float(pred_x32[row_index])
        (
            suggest_close,
            exit_reason,
            pred_linear,
            last_renew_segment_evaluated,
        ) = _evaluate_sign_only_checkpoint(
            side=side,
            bars_held=bars_held,
            min_hold_steps=min_hold_steps,
            check_interval_steps=check_interval_steps,
            pred_log2=pred_log2,
            last_renew_segment_evaluated=last_renew_segment_evaluated,
        )
        row = {
            'bars_held': bars_held,
            'target_sample_index': target_sample_index,
            'matched_sample_index': int(sample_index[row_index]),
            'sample_index_miss': int(sample_index[row_index] - target_sample_index),
            'matched_entry_start_trade_id': int(entry_tids[row_index]),
            'pred_x32_log2': pred_log2,
            'pred_x32_pct': _log2_to_pct(pred_log2),
            'pred_eval_linear': pred_linear,
            'suggest_close': suggest_close,
            'exit_reason': exit_reason,
            'last_renew_segment_evaluated': last_renew_segment_evaluated,
        }
        checkpoint_rows.append(row)
        if suggest_close:
            close_bars.append(bars_held)

    return {
        'npz_path': str(npz_path),
        'entry_start_trade_id': entry_start_trade_id,
        'side': side,
        'min_hold_steps': min_hold_steps,
        'check_interval_steps': check_interval_steps,
        'estimated_entry_sample_index': entry_sample_index,
        'checkpoint_rows': checkpoint_rows,
        'suggest_close_bars': close_bars,
        'suggest_close_count': len(close_bars),
    }


def _load_open_position_from_journal() -> dict[str, object]:
    journal = get_journal_state()
    open_position = journal['open_position']
    if open_position is None:
        raise RuntimeError('Journal has no open position')
    return open_position


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Probe sign_only renew checkpoints from trade_research NPZ',
    )
    parser.add_argument(
        '--npz-path',
        type=Path,
        required=True,
    )
    parser.add_argument(
        '--entry-start-trade-id',
        type=int,
        default=None,
    )
    parser.add_argument(
        '--side',
        type=str,
        default=None,
    )
    parser.add_argument(
        '--min-hold-steps',
        type=int,
        default=None,
    )
    parser.add_argument(
        '--check-interval-steps',
        type=int,
        default=32,
    )
    parser.add_argument(
        '--max-bars',
        type=int,
        default=704,
    )
    parser.add_argument(
        '--output-json',
        type=Path,
        default=None,
    )
    args = parser.parse_args()

    open_position = None
    if (
        args.entry_start_trade_id is None
        or args.side is None
        or args.min_hold_steps is None
    ):
        open_position = _load_open_position_from_journal()

    entry_start_trade_id = args.entry_start_trade_id
    if entry_start_trade_id is None:
        entry_start_trade_id = int(open_position['entry_start_trade_id'])

    side = args.side
    if side is None:
        side = str(open_position['side'])

    min_hold_steps = args.min_hold_steps
    if min_hold_steps is None:
        min_hold_steps = int(open_position['exit_stack_min_hold_steps'])

    report = probe_sign_only_renew_checkpoints(
        npz_path=args.npz_path,
        entry_start_trade_id=entry_start_trade_id,
        side=side,
        min_hold_steps=min_hold_steps,
        check_interval_steps=args.check_interval_steps,
        max_bars=args.max_bars,
    )

    print(json.dumps(report, indent=2, sort_keys=True))
    if args.output_json is not None:
        args.output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True),
            encoding='utf-8',
        )


if __name__ == '__main__':
    main()
