def load_dolly15k(rec: dict) -> str:
    """
    Loader function to extract text from a Dolly dataset record.

    Args:
        rec (dict): A record from the Dolly dataset.

    Returns:
        str: The extracted text content.
    """
    prompt = rec.get("prompt", "")
    context = rec.get("context", "")
    response = rec.get("response", "")

    full_text = f"{prompt}\n{context}\n{response}".strip()
    return full_text