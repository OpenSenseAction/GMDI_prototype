"""
Basic xarray-based workflow using pycomlink algorithms.
Follows the pattern from pycomlink example notebooks.
"""

from .base import BaseRainWorkflow
import xarray as xr
from datetime import datetime, timezone
import logging
import numpy as np

logger = logging.getLogger(__name__)


class OpenMRGBasicWorkflow(BaseRainWorkflow):
    """
    Basic xarray-based workflow using pycomlink algorithms.

    Processing steps follow pycomlink example:
    1. Calculate total loss (TL = TSL - RSL)
    2. Wet/dry classification using rolling std > 0.8
    3. Baseline estimation using pycomlink baseline_constant
    4. WAA using pycomlink waa_schleiss_2013
    5. Rain-induced attenuation: A = TL - baseline - WAA
    6. Rain rate using ITU-R power-law with frequency/polarization
    """

    def process(
        self, ds_cmls: xr.Dataset, window_start: datetime, window_end: datetime
    ) -> xr.Dataset:
        """
        Process CML data to rain rates.

        See BaseRainWorkflow.process() for parameter/return documentation.
        """
        import pycomlink as pycml

        # calculate total loss
        ds_cmls["tl"] = ds_cmls.tsl - ds_cmls.rsl
        # seperate periods of rain from dry time steps
        ds_cmls["wet"] = (
            ds_cmls.tl.rolling(time=60 * 6, center=True).std(skipna=False) > 1.0
        )
        # estiamte the baseline during rain events
        ds_cmls["baseline"] = pycml.processing.baseline.baseline_constant(
            trsl=ds_cmls.tl,
            wet=ds_cmls.wet,
            n_average_last_dry=5,
        )
        # compenmsate for wet antenna attenuation
        ds_cmls["waa"] = pycml.processing.wet_antenna.waa_schleiss_2013(
            rsl=ds_cmls.tl,
            baseline=ds_cmls.baseline,
            wet=ds_cmls.wet,
            waa_max=2.2,
            delta_t=1,
            tau=15,
        )
        # calculate attenuation caused by rain and remove negative attenuation
        ds_cmls["a_rain"] = ds_cmls.tl - ds_cmls.baseline - ds_cmls.waa
        ds_cmls["a_rain"].values[ds_cmls.a_rain < 0] = 0
        # derive rain rate via the k-R relation
        ds_cmls["r"] = pycml.processing.k_R_relation.calc_R_from_A(
            A=ds_cmls.a_rain,
            L_km=ds_cmls.length.astype(float) / 1000,  # convert to km
            f_GHz=ds_cmls.frequency / 1000,  # convert to GHz
            pol=ds_cmls.polarization,
        )
        ds_cmls["r"].values[ds_cmls.r < 0.1] = 0

        # Trim output to target persistence window
        if window_start is not None and window_end is not None:
            try:
                ds_cmls = ds_cmls.sel(time=slice(window_start, window_end))
            except Exception as e:
                logger.warning(
                    f"Could not trim to time window [{window_start}, {window_end}]: {e}"
                )

        return ds_cmls[["tl", "wet", "baseline", "waa", "a_rain", "r"]]
