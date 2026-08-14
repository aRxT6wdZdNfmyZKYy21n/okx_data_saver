"""Build StatefulHybridTradeDatasetInference config from inference_api metadata."""

from __future__ import annotations

from main.offline_inference.trading_bot_imports import ensure_trading_bot_on_path

ensure_trading_bot_on_path()

from omegaconf import OmegaConf

from trading_bot_dataset.src.stateful_inference_dataset import (
    StatefulHybridTradeDatasetInference,
    StatefulInferenceConfig,
)
from trading_bot_dataset.src.volume_windows import extract_volume_windows_config


def build_stateful_inference_config(
    metadata: dict[str, object],
) -> StatefulInferenceConfig:
    dataset_cfg = metadata['dataset_config']
    model_cfg_raw = metadata['model_config']
    dataset_cfg_omega = OmegaConf.create(dataset_cfg)
    model_cfg_omega = OmegaConf.create(
        {
            'params': {
                'scale_features': model_cfg_raw['scale_features'],
            },
        },
    )

    dataset_tag = 'inference_default'
    if 'profile' in metadata:
        dataset_tag = str(metadata['profile'])
    elif 'tag' in dataset_cfg:
        dataset_tag = str(dataset_cfg['tag'])

    range_hold_cap_multiplier = None
    if 'range_hold_cap_multiplier' in dataset_cfg:
        if dataset_cfg['range_hold_cap_multiplier'] is not None:
            range_hold_cap_multiplier = int(dataset_cfg['range_hold_cap_multiplier'])

    coarse_phase_mode = 'fixed'
    if 'coarse_phase_mode' in dataset_cfg:
        coarse_phase_mode = str(dataset_cfg['coarse_phase_mode'])

    coarse_phase_offset = 0
    if 'coarse_phase_offset' in dataset_cfg:
        coarse_phase_offset = int(dataset_cfg['coarse_phase_offset'])

    coarse_phase_offset_max = None
    if 'coarse_phase_offset_max' in dataset_cfg:
        if dataset_cfg['coarse_phase_offset_max'] is not None:
            coarse_phase_offset_max = int(dataset_cfg['coarse_phase_offset_max'])

    return StatefulHybridTradeDatasetInference.build_config(
        dataset_tag=dataset_tag,
        sequence_length=int(metadata['sequence_length']),
        raw_columns=list(dataset_cfg['raw_cols']),
        static_columns=list(dataset_cfg['static_cols']),
        target_cols=list(dataset_cfg['target_cols']),
        aggregation_levels=list(dataset_cfg['aggregation_levels']),
        use_indicators=bool(dataset_cfg['use_indicators']),
        indicator_cols=list(dataset_cfg['indicator_cols']),
        volume_windows_config=extract_volume_windows_config(dataset_cfg_omega),
        range_hold_cap_multiplier=range_hold_cap_multiplier,
        coarse_phase_mode=coarse_phase_mode,
        coarse_phase_offset=coarse_phase_offset,
        coarse_phase_offset_max=coarse_phase_offset_max,
        model_config=model_cfg_omega,
    )
