import os

import google.generativeai as genai
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("GEMINI_API_KEY is not set. Please add it to .env or export it.")
        return

    genai.configure(api_key=api_key)
    print("Listing models with generateContent support:\n")
    try:
        models = genai.list_models()
    except Exception as exc:
        print(f"Failed to list models: {exc}")
        return

    available = [
        m for m in models if "generateContent" in getattr(m, "supported_generation_methods", [])
    ]
    if not available:
        print("No models with generateContent support found for this API key.")
        return

    for model in available:
        methods = ", ".join(model.supported_generation_methods or [])
        print(f"{model.name} -> {methods}")


if __name__ == "__main__":
    main()
