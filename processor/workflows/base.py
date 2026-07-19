"""
Abstract base class for rain rate processing workflows.
All workflow variants must inherit from this class and implement process().
"""

from abc import ABC, abstractmethod
import xarray as xr
from datetime import datetime


class BaseRainWorkflow(ABC):
    """
    Abstract base class for rain rate processing workflows.
    All workflow variants must inherit from this class and implement process().
    """

    @abstractmethod
    def process(
        self, cml_ds: xr.Dataset, window_start: datetime, window_end: datetime
    ) -> xr.Dataset:
        """
        Process raw CML data to estimate rain rates.

        Args:
            cml_ds: Canonical xarray dataset with dimensions [time, cml_id, sublink_id]
                    and variables including [rsl, tsl] plus metadata variables.
                    May contain NaN values and missing metadata.

            window_start: Start of processing window (for context)
            window_end: End of processing window

        Returns:
            xr.Dataset with dimensions [time, cml_id, sublink_id] and variables:
            - tl: total loss (TSL - RSL)
            - wet: boolean wet/dry classification
            - baseline: baseline attenuation level
            - waa: wet antenna attenuation estimate
            - a_rain: rain-induced attenuation
            - r: rain rate estimate (mm/h)

            The dataset should preserve coordinates and carry `user_id` in attrs.
            All fields can be NULL/NaN if processing fails for a timestamp.

        Notes:
            - Implementations should be robust to missing metadata
            - Should handle gaps in time series gracefully
            - Should not raise exceptions on bad data (log warnings, return partial results)
            - Processing time window may be larger than output window (for temporal context)
            - Output should usually be trimmed to the target interval that should be persisted,
              even if a larger context window was used internally
        """
        pass

    def get_name(self) -> str:
        """Return workflow name for logging."""
        return self.__class__.__name__
