import csv
import os
from pathlib import Path
import shutil
import kagglehub

# Download selected CSVs from Kaggle, store full files in data/full,
# then write small samples to data/sample.

def write_samples(full_dir: Path, sample_dir: Path, files: list[str], sample_rows: int) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    for name in files:
        src = full_dir / name
        dst = sample_dir / name
        with src.open("r", encoding="utf-8", newline="") as f_in, dst.open(
            "w", encoding="utf-8", newline=""
        ) as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()
            for i, row in enumerate(reader):
                if i >= sample_rows:
                    break
                writer.writerow(row)
        print(f"Wrote sample {dst} ({sample_rows} rows)")


def download_datasets() -> None:
    dataset_path = Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))
    output_dir = Path("data/full")
    sample_dir = Path("data/sample")
    output_dir.mkdir(parents=True, exist_ok=True)
    files_to_download = [
        "olist_customers_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_products_dataset.csv",
    ]
    sample_rows = int(os.getenv("SAMPLE_ROWS", "1000"))

    for file_name in files_to_download:
        src = dataset_path / file_name
        if not src.exists():
            matches = list(dataset_path.rglob(file_name))
            if not matches:
                raise FileNotFoundError(f"Could not find {file_name} under {dataset_path}")
            src = matches[0]
        shutil.copy2(src, output_dir / file_name)
        print(f"Copied {file_name} to {output_dir}")

    write_samples(output_dir, sample_dir, files_to_download, sample_rows)
    print("All datasets downloaded successfully.")


if __name__ == "__main__":
    download_datasets()
