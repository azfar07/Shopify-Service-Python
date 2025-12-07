def normalize_sku(value):
    if not value:
        return ""
    return str(value).strip().replace(" ", "").replace("#", "").upper()

def normalize_name(value):
    if not value:
        return ""
    return " ".join(str(value).split()).strip()

def normalize_description(value):
    return value.strip() if value else ""
