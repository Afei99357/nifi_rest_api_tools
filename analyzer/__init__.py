"""
NiFi Processor Usage Analyzer

Lightweight tool for analyzing NiFi processor execution frequency
to identify unused or underutilized processors for pruning decisions.
"""

from .nifi_client import NiFiClient, NiFiAuthError, NiFiClientError, NiFiNotFoundError
from .usage_analyzer import ProcessorUsageAnalyzer

__version__ = "0.1.0"

__all__ = [
    "NiFiClient",
    "NiFiAuthError",
    "NiFiClientError",
    "NiFiNotFoundError",
    "ProcessorUsageAnalyzer",
]
