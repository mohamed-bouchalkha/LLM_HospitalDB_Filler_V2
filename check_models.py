import google.generativeai as genai
import os

# Make sure your API key is set
api_key = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key="AIzaSyD7zpM4kmggQaKr1rXkeS8k9r4YC_lku-U")

print("Available Models:")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(f"- {m.name}")