"""
Plotting utilities for rain rate processing validation.
Provides standard visualization functions for workflow results.
"""

import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
from typing import Optional


def select_single_link(
    ds: xr.Dataset, cml_id: Optional[str] = None, sublink_id: Optional[str] = None
) -> xr.Dataset:
    """
    Return a dataset slice for one link for plotting.

    Args:
        ds: Input xarray dataset with dimensions [time, cml_id, sublink_id]
        cml_id: CML ID to select (None = first available)
        sublink_id: Sublink ID to select (None = first available)

    Returns:
        Sliced dataset with only time dimension
    """
    if ds.dims.get("time", 0) == 0:
        return ds

    # Auto-select if not specified
    if cml_id is None:
        cml_id = ds.coords["cml_id"].values[0]
    if sublink_id is None:
        sublink_id = ds.coords["sublink_id"].values[0]

    try:
        return ds.sel(cml_id=cml_id, sublink_id=sublink_id)
    except Exception as e:
        raise ValueError(
            f"Could not select link (cml_id={cml_id}, sublink_id={sublink_id}): {e}"
        )


def plot_rain_workflow_overview(
    ds: xr.Dataset,
    cml_id: Optional[str] = None,
    sublink_id: Optional[str] = None,
    title: Optional[str] = None,
    figsize: tuple = (14, 18),
) -> plt.Figure:
    """
    Create a standard multi-panel plot for tl, wet, baseline, waa, a_rain, r.

    Args:
        ds: Processed rain dataset with variables [tl, wet, baseline, waa, a_rain, r]
        cml_id: CML ID to plot (None = first available)
        sublink_id: Sublink ID to plot (None = first available)
        title: Plot title (None = auto-generated)
        figsize: Figure size in inches

    Returns:
        Matplotlib figure object
    """
    # Select single link
    link_ds = select_single_link(ds, cml_id, sublink_id)

    if link_ds.dims.get("time", 0) == 0:
        raise ValueError("No data available for plotting")

    # Get selected coordinates for title
    plot_cml_id = link_ds.attrs.get("selected_cml_id", str(cml_id))
    plot_sublink_id = link_ds.attrs.get("selected_sublink_id", str(sublink_id))

    if title is None:
        title = f"Rain Rate Processing Results - CML {plot_cml_id}, Sublink {plot_sublink_id}"

    # Create figure with 6 panels
    fig, axes = plt.subplots(6, 1, figsize=figsize, sharex=True)
    fig.suptitle(title, fontsize=14, fontweight="bold")

    time = link_ds.time.values

    # Panel 1: Total Loss (TL)
    axes[0].plot(time, link_ds["tl"].values, "b-", linewidth=1, label="TL")
    axes[0].set_ylabel("TL (dB)")
    axes[0].set_title("Total Loss (TSL - RSL)")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="upper right")

    # Panel 2: Wet/Dry Classification
    wet_values = link_ds["wet"].values.astype(int)
    axes[1].fill_between(
        time, 0, 1, where=wet_values, alpha=0.5, color="blue", label="Wet"
    )
    axes[1].fill_between(
        time, 0, 1, where=~wet_values, alpha=0.5, color="gray", label="Dry"
    )
    axes[1].set_yticks([0, 1])
    axes[1].set_yticklabels(["Dry", "Wet"])
    axes[1].set_ylabel("Classification")
    axes[1].set_title("Wet/Dry Classification")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc="upper right")

    # Panel 3: Baseline
    axes[2].plot(time, link_ds["tl"].values, "b-", linewidth=0.5, alpha=0.5, label="TL")
    axes[2].plot(time, link_ds["baseline"].values, "r-", linewidth=2, label="Baseline")
    axes[2].set_ylabel("Attenuation (dB)")
    axes[2].set_title("Baseline Estimation")
    axes[2].grid(True, alpha=0.3)
    axes[2].legend(loc="upper right")

    # Panel 4: Wet Antenna Attenuation (WAA)
    axes[3].plot(time, link_ds["waa"].values, "g-", linewidth=1, label="WAA")
    axes[3].set_ylabel("WAA (dB)")
    axes[3].set_title("Wet Antenna Attenuation")
    axes[3].grid(True, alpha=0.3)
    axes[3].legend(loc="upper right")

    # Panel 5: Rain-Induced Attenuation (A_rain)
    axes[4].plot(time, link_ds["a_rain"].values, "m-", linewidth=1, label="A_rain")
    axes[4].set_ylabel("A_rain (dB)")
    axes[4].set_title("Rain-Induced Attenuation")
    axes[4].grid(True, alpha=0.3)
    axes[4].legend(loc="upper right")

    # Panel 6: Rain Rate (R)
    axes[5].plot(time, link_ds["r"].values, "r-", linewidth=1.5, label="R")
    axes[5].set_ylabel("R (mm/h)")
    axes[5].set_xlabel("Time")
    axes[5].set_title("Rain Rate Estimate")
    axes[5].grid(True, alpha=0.3)
    axes[5].legend(loc="upper right")

    # Rotate x-axis labels
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    return fig


def summarize_rain_dataset(ds: xr.Dataset) -> dict:
    """
    Return simple summary statistics for validation.

    Args:
        ds: Processed rain dataset

    Returns:
        Dictionary with summary statistics
    """
    if ds.dims.get("time", 0) == 0:
        return {"error": "No data available"}

    wet = ds["wet"].values
    rain = ds["r"].values

    # Filter out NaN values
    rain_non_nan = rain[~np.isnan(rain)]
    wet_count = int(np.nansum(wet))
    total_count = int(wet.size)

    stats = {
        "total_observations": total_count,
        "wet_observations": wet_count,
        "dry_observations": total_count - wet_count,
        "wet_percentage": 100.0 * wet_count / total_count if total_count > 0 else 0.0,
        "rain_min": float(np.min(rain_non_nan)) if len(rain_non_nan) > 0 else None,
        "rain_max": float(np.max(rain_non_nan)) if len(rain_non_nan) > 0 else None,
        "rain_mean": float(np.mean(rain_non_nan)) if len(rain_non_nan) > 0 else None,
        "rain_median": (
            float(np.median(rain_non_nan)) if len(rain_non_nan) > 0 else None
        ),
        "rain_std": float(np.std(rain_non_nan)) if len(rain_non_nan) > 0 else None,
    }

    return stats
