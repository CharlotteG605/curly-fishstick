import json

def to_plain_context(obj, indent=0):
    """
    Recursively converts JSON object into a plain readable text block.
    """
    plain_text = ""
    space = "  " * indent

    if isinstance(obj, dict):
        for key, value in obj.items():
            plain_text += f"{space}{key}:"
            if isinstance(value, (dict, list)):
                plain_text += "\n" + to_plain_context(value, indent + 1)
            else:
                plain_text += f" {value}\n"

    elif isinstance(obj, list):
        for i, item in enumerate(obj, 1):
            plain_text += f"{space}- Item {i}:\n{to_plain_context(item, indent + 1)}"

    else:  # base case: string, number, boolean, etc
        plain_text += f"{space}{obj}\n"

    return plain_text


if __name__ == "__main__":
    # Use your full Windows path
    file_path = r"C:\Users\charlottegong\.vscode\technical_seo_audit_results.json"

    # Load the JSON file
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Convert to plain context
    plain_output = to_plain_context(data)

    # Save as txt in the same folder
    output_path = r"C:\Users\charlottegong\.vscode\seo_audit_plain.txt"
    with open(output_path, "w", encoding="utf-8") as out:
        out.write(plain_output)

    print(f"Plain context has been written to {output_path}")
