"""
Tiny helper to make pandas/numpy payloads round-trip through strict JSON.

The sandbox returns `result = {"detail_rows": df.to_dict('records'), ...}` and
pandas freely yields `float('nan')` / `inf` for missing cells or divide-by-zero.
`json.dumps(..., allow_nan=False)` — which FastAPI, Pydantic, and requests all
use at various boundaries — refuses those values with:

    ValueError: Out of range float values are not JSON compliant: nan

Call `sanitize_for_json(obj)` once on anything that will cross such a boundary.
"""
from __future__ import annotations

import math
from typing import Any


def sanitize_for_json(obj: Any) -> Any:
    """Recursively return a deep copy of `obj` with NaN/Inf floats turned into None.

    * dict keys that are non-str are coerced via str()
    * pandas `NaT` / numpy scalar NaNs surface as Python floats, so the float
      branch catches them
    * tuples become lists (strict JSON has no tuple concept)
    """
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {
            (k if isinstance(k, str) else str(k)): sanitize_for_json(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    # Numpy / pandas scalar types: coerce to native Python.
    try:
        import numpy as np
        if isinstance(obj, np.floating):
            val = float(obj)
            if math.isnan(val) or math.isinf(val):
                return None
            return val
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
    except Exception:
        pass
    return obj
