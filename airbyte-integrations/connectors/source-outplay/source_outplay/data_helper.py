from datetime import date, datetime
from typing import Any, Mapping, Optional

import pendulum


def retrieve_date_from_mapping(mapping: Optional[Mapping[str, Any]], key: Optional[str]) -> Optional[date]:
    mapping_value = mapping.get(key, None) if mapping is not None and key is not None else None
    if isinstance(mapping_value, str):
        return pendulum.parse(mapping_value, strict=False).date()
    elif isinstance(mapping_value, date):
        return mapping_value
    elif isinstance(mapping_value, datetime):
        return mapping_value.date()
    else:
        return None
