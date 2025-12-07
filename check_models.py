import google.generativeai as genai
import os

# Make sure your API key is set
api_key = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key="YOUR_API_KEY")

print("Available Models:")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(f"- {m.name}")
