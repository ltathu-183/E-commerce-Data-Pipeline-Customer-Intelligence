#!/usr/bin/env python3
"""
Verify Fresh Setup: Simulates the experience of cloning this repo for the first time.
Run this AFTER following README Quick Start to validate everything works end-to-end.
"""
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: str, desc: str) -> bool:
    """Run command, print status, return success"""
    print(f"\n🔹 {desc}")
    print(f"   Command: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, check=True, cwd=Path(__file__).parent.parent
        )
        print("   ✅ Success")
        if result.stdout.strip():
            print(f"   Output: {result.stdout.strip()[:200]}...")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed: {e.stderr.strip()[:200]}")
        return False

def main():
    print("🚀 Fresh Setup Verification Suite")
    print("=" * 60)

    checks = [
        # 1. Environment check
        ("python --version", "Check Python version"),
        ("uv --version", "Check uv installed"),
        ("docker --version", "Check Docker installed"),

        # 2. Dependency install
        ("uv pip install -e .", "Install project dependencies"),

        # 3. Start database
        ("docker-compose up -d db", "Start PostgreSQL container"),
        ("timeout 15 docker-compose logs db | grep 'ready'", "Wait for DB to be ready"),

        # 4. Run ETL pipeline
        ("$env:USE_DATABASE='true'; python src/etl_pipeline.py" if sys.platform == "win32"
         else "USE_DATABASE=true python src/etl_pipeline.py", "Run ETL pipeline"),

        # 5. Verify outputs exist
        ("test -f data/processed/dwh/fact_order_items.csv", "Check fact table CSV exists"),
        ("test -f data/processed/dwh/dim_customers.csv", "Check dimension CSV exists"),

        # 6. Run tests
        ("pytest tests/test_etl.py -v -m 'not integration'", "Run unit tests"),

        # 7. Cleanup
        ("docker-compose down", "Stop containers"),
    ]

    results = []
    for cmd, desc in checks:
        success = run_cmd(cmd, desc)
        results.append((desc, success))
        if not success and "Run ETL" in desc:
            print("\n⚠️  Pipeline failed — check logs above. Stopping verification.")
            break

    # Summary
    print("\n" + "=" * 60)
    print("📊 Verification Summary")
    print("=" * 60)
    passed = sum(1 for _, s in results if s)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    for desc, success in results:
        status = "✅" if success else "❌"
        print(f"{status} {desc}")

    if passed == total:
        print("\n🎉 All checks passed! Your project is ready for portfolio submission.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} check(s) failed. Review output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
