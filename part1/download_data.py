import subprocess
from tempfile import TemporaryDirectory
from shutil import unpack_archive, move
from pathlib import Path
import wget
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


BTS_ROOT_URL = "https://transtats.bts.gov/PREZIP"


def download_monthly_data(year: int, month: int, ouput_dir: Path) -> Path:
    file_download_url = f"{BTS_ROOT_URL}/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

    output_file_path = ouput_dir / f"{year}_{month:02}.zip"
    output_file_path.parent.mkdir(exist_ok=True, parents=True)

    wget.download(file_download_url, output_file_path.as_posix())

    return output_file_path


def extract_zipfile(zip_file: Path, output_dir: Path) -> Path:
    with TemporaryDirectory() as tmpdir:
        unpack_archive(zip_file, extract_dir=tmpdir)

        csv_files = list(Path(tmpdir).glob("*.csv"))
        assert len(csv_files) == 1, "There should be exactly one *.csv file per zip"

        year = int(zip_file.name.split(".")[0].split("_")[-2])
        month = int(zip_file.name.split(".")[0].split("_")[-1])
        output_file = output_dir / f"{year}" / f"{year}-{month:02}.csv"

        output_file.parent.mkdir(exist_ok=True, parents=True)

        move(csv_files[0], output_file)

        return output_file


def extract_data(input_dir: Path, output_dir: Path):
    for zip_filepath in input_dir.glob("*.zip"):
        output_file = extract_zipfile(zip_filepath, output_dir)
        print("Extracted:", output_file)


if __name__ == "__main__":
    INPUT_DIR = Path("./data/raw")
    OUTPUT_DIR = Path("./data/processed")
    BUCKET = "XXX"  # TODO: Replace with your bucket name

    download_monthly_data(2021, 12, INPUT_DIR)
    extract_data(INPUT_DIR, OUTPUT_DIR)
    subprocess.check_call(["gsutil", "cp", "-r", OUTPUT_DIR.absolute(), f"gs://{BUCKET}/data"])
