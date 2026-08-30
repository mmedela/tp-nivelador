import argparse
from pathlib import Path


SERVER_SERVICE = """services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=server
      - SERVER_PORT=5678
      - AGENCY_QUORUM_MIN=6
      - GRACE_TIME=4
    ports:
      - "5678:5678"
"""

CLIENT_SERVICE = """
  client_{agency_id}:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_{agency_id}
    depends_on:
      - server
    environment:
      - AGENCY_ID={agency_id}
      - SERVER_HOST=server
      - SERVER_PORT=5678
      - INPUT_FILE=/input/input-{agency_id}.csv
      - OUTPUT_FILE=/output/output-{agency_id}.csv
    volumes:
      - ./input:/input:ro
      - ./output:/output
"""


def positive_int(value: str) -> int:
    try:
        client_count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("client count must be an integer") from exc

    if client_count < 1:
        raise argparse.ArgumentTypeError("client count must be greater than 0")

    return client_count


def build_compose(client_count: int) -> str:
    clients = "".join(
        CLIENT_SERVICE.format(agency_id=agency_id)
        for agency_id in range(client_count)
    )
    return SERVER_SERVICE + clients


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yaml with the requested client count."
    )
    parser.add_argument(
        "client_count",
        type=positive_int,
        help="number of client containers to configure",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=repo_root / "docker-compose.yaml",
        help="output file path, defaults to the repository docker-compose.yaml",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output.write_text(build_compose(args.client_count), encoding="utf-8")
    print(f"Generated {args.output} with {args.client_count} clients")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
