"""Pydantic models for the Reporting Agent API."""
from __future__ import annotations

from enum import Enum


class ProjectType(str, Enum):
    NTM = "NTM"
    AHLOB_MODERNIZATION = "AHLOB Modernization"
    BOTH = "Both"
