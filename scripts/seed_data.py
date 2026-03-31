from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    seed_path = root / "data" / "seeds" / "baseline_metrics.json"
    data = json.loads(seed_path.read_text(encoding="utf-8"))
    print("Loaded baseline seed metrics:")
    for key, value in data.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
