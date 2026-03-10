import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.domain_extractor.market_annotations import build_and_save_market_annotations

def main():
    build_and_save_market_annotations()

if __name__ == "__main__":
    main()
