"""
Registry for mapping processing variant names to workflow classes.
"""

from typing import Dict, Type
from workflows.base import BaseRainWorkflow
from workflows.default import DefaultRainWorkflow
from workflows.openmrg_basic import OpenMRGBasicWorkflow


class WorkflowRegistry:
    """
    Registry mapping processing variant names to workflow classes.
    """

    _registry: Dict[str, Type[BaseRainWorkflow]] = {
        "default": DefaultRainWorkflow,
        "openmrg_basic": OpenMRGBasicWorkflow,
    }

    @classmethod
    def get_workflow(cls, variant_name: str) -> BaseRainWorkflow:
        """
        Get a workflow instance by variant name.

        Args:
            variant_name: Name from config (e.g., 'default')

        Returns:
            Instance of the workflow class

        Raises:
            ValueError: If variant_name not in registry
        """
        workflow_class = cls._registry.get(variant_name)
        if workflow_class is None:
            raise ValueError(
                f"Unknown processing variant: {variant_name}. "
                f"Available: {list(cls._registry.keys())}"
            )
        return workflow_class()

    @classmethod
    def register(cls, variant_name: str, workflow_class: Type[BaseRainWorkflow]):
        """
        Register a new workflow variant.

        Args:
            variant_name: Name to use in config
            workflow_class: Class inheriting from BaseRainWorkflow
        """
        cls._registry[variant_name] = workflow_class

    @classmethod
    def list_variants(cls) -> list:
        """Return list of registered variant names."""
        return list(cls._registry.keys())
