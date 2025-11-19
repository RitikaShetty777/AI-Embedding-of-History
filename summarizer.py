# summarizer.py
"""
Reads SUM_INPUT_CSV, generates ai_summary for each row using OpenAI,
writes SUM_OUTPUT_CSV. Resumable: if output exists it merges existing ai_summary.
Config via .env
"""

import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm

from openai import OpenAI, APIError, RateLimitError, APITimeoutError, InternalServerError

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
INPUT_CSV = os.getenv("SUM_INPUT_CSV", "scraped_raw.csv")
OUTPUT_CSV = os.getenv("SUM_OUTPUT_CSV", "output_with_summaries.csv")

SAVE_EVERY = int(os.getenv("SUM_SAVE_EVERY", "10"))
MAX_RETRIES = int(os.getenv("SUM_MAX_RETRIES", "6"))
INITIAL_BACKOFF = float(os.getenv("SUM_BACKOFF_INITIAL", "1.0"))
MAX_TOKENS = int(os.getenv("SUM_MAX_TOKENS", "120"))
TEMPERATURE = float(os.getenv("SUM_TEMPERATURE", "0.2"))
DELAY_BETWEEN_CALLS = float(os.getenv("SUM_DELAY", "0.2"))

if not OPENAI_API_KEY:
    raise SystemExit("OPENAI_API_KEY is not set (put it in .env or environment)")

client = OpenAI(api_key=OPENAI_API_KEY)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("summarizer")

SYSTEM_PROMPT = (
    "You are an assistant that writes a short 1-2 sentence factual summary (<=40 words) "
    "for a historical page using only the provided fields."
)


def build_messages(row):
    content = (
        f"title: {row.get('title','')}\n"
        f"url: {row.get('url','')}\n"
        f"date: {row.get('date','')}\n"
        f"author: {row.get('author','')}\n"
        f"category: {row.get('category','')}\n"
        f"excerpt: {row.get('excerpt','')}\n"
        f"content_length: {row.get('content_length','')}\n"
        f"tags: {row.get('tags','')}\n"
        f"notes: {row.get('notes','')}\n\n"
        "Write a concise abstract (<= 40 words)."
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": content}
    ]


def call_openai(messages, retries=MAX_RETRIES):
    backoff = INITIAL_BACKOFF
    for attempt in range(1, retries + 1):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE
            )
            return resp.choices[0].message.content.strip()
        except (RateLimitError, APITimeoutError, InternalServerError, APIError) as e:
            if attempt == retries:
                logger.error("OpenAI retries exhausted: %s", e)
                return ""
            sleep = backoff * (2 ** (attempt - 1))
            logger.warning("OpenAI error, retrying in %.1fs (%s)", sleep, e)
            time.sleep(sleep)
        except Exception as e:
            logger.exception("Unexpected OpenAI error: %s", e)
            return ""


def main():
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_CSV)
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path.resolve()}")

    df = pd.read_csv(input_path, dtype=str).fillna("")

    # If output exists, merge existing ai_summary
    if output_path.exists():
        out_df = pd.read_csv(output_path, dtype=str).fillna("")
        if "ai_summary" in out_df.columns:
            df = df.merge(out_df[["url", "ai_summary"]], on="url", how="left")
    else:
        df["ai_summary"] = ""

    total = len(df)
    logger.info("Rows to process: %d", total)

    for idx in tqdm(range(total), desc="Summarizing"):
        if df.at[idx, "ai_summary"]:
            continue
        row = df.iloc[idx].to_dict()
        messages = build_messages(row)
        summary = call_openai(messages)
        df.at[idx, "ai_summary"] = summary
        time.sleep(DELAY_BETWEEN_CALLS)
        if (idx + 1) % SAVE_EVERY == 0 or (idx + 1) == total:
            df.to_csv(output_path, index=False)
            logger.info("Saved progress to %s (%d/%d)", output_path, idx + 1, total)

    df.to_csv(output_path, index=False)
    logger.info("All summaries complete → %s", output_path)


if __name__ == "__main__":
    main()
