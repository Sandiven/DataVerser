from bson import ObjectId

def clean_mongo_document(doc):
    """
    Recursively convert all ObjectId fields into strings so FastAPI can return them safely.
    """

    if doc is None:
        return None

    # Case: dict
    if isinstance(doc, dict):
        cleaned = {}
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                cleaned[key] = str(value)
            else:
                cleaned[key] = clean_mongo_document(value)
        return cleaned

    # Case: list
    if isinstance(doc, list):
        return [clean_mongo_document(item) for item in doc]

    # Base case: normal value
    return doc
