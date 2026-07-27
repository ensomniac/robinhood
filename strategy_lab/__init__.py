"""Compact, observable strategy discovery and validation platform."""

from .config import LabConfig, load_config
from .contracts import CandidateState, StrategySpec

__all__ = ["CandidateState", "LabConfig", "StrategySpec", "load_config"]
__version__ = "1.0.0"
