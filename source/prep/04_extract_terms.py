import os
import re
import pandas as pd
from PyPDF2 import PdfReader
from pathlib import Path
from docling.document_converter import DocumentConverter

# -------------------------------------------------------------------
# 1. Paths – adjust these if your project name/folders differ
# -------------------------------------------------------------------
BASE_DIR = Path.cwd().parents[1]
DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
DATA_HELPER = os.path.join(BASE_DIR, "data", "helper")
DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")

EXCEL_PATH = os.path.join(DATA_HELPER, "Four-Pillars_Network-Variables.xlsx")

# -------------------------------------------------------------------
# 2. Helpers
# -------------------------------------------------------------------
def load_terms_from_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)

    records = []
    for col in df.columns:
        for value in df[col].dropna().unique():
            term = str(value).strip()
            if term:
                records.append({"category": col, "term": term})

    terms_df = pd.DataFrame(records).drop_duplicates().reset_index(drop=True)
    return terms_df

def pdf_to_text(path: str) -> str:
    converter = DocumentConverter()
    result = converter.convert(path)

    text = result.document.export_to_text()

    return text


def count_term(text: str, term: str) -> int:
    pattern = re.escape(term)
    return len(re.findall(pattern, text, flags=re.IGNORECASE))


# -------------------------------------------------------------------
# 3. Main pipeline
# -------------------------------------------------------------------
def main():
    os.makedirs(DATA_PROCESSED, exist_ok=True)

    # 3.1 Load terms from Excel
    terms_df = load_terms_from_excel(EXCEL_PATH)
    print(f"Loaded {len(terms_df)} terms from Excel.")

    # 3.2 Read PDFs
    pdf_files = [
        f for f in os.listdir(DATA_RAW)
        if f.lower().endswith(".pdf")
    ]
    if not pdf_files:
        print("No PDF files found in data/raw.")
        return

    print("PDF files found:")
    for f in pdf_files:
        print("  -", f)

    # 3.3 Count matches
    results = []

    for pdf_name in pdf_files:
        pdf_path = os.path.join(DATA_RAW, pdf_name)
        print(f"\nReading {pdf_name} ...")
        text = pdf_to_text(pdf_path)

        for _, row in terms_df.iterrows():
            category = row["category"]
            term = row["term"]
            c = count_term(text, term)
            if c > 0:
                results.append({
                    "document": pdf_name,
                    "category": category,
                    "term": term,
                    "count": c
                })

    # 3.4 Save results
    if not results:
        print("No matches found. Check terms or PDFs.")
        return

    results_df = pd.DataFrame(results)
    output_path = os.path.join(DATA_PROCESSED, "policy_term_counts.csv")
    results_df.to_csv(output_path, index=False)

    print("\nSaved results to:", output_path)
    print(results_df.head())


if __name__ == "__main__":
    main()
