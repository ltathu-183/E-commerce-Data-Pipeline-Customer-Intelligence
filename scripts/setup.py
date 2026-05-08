#!/usr/bin/env python3
"""
Automated Setup Script for E-Commerce Data Pipeline
===================================================

Ensures complete reproducibility by automating:
1. Environment setup
2. Dependency installation
3. Infrastructure startup
4. Data pipeline execution
5. Validation checks

Usage: python scripts/setup.py
"""

import subprocess
import sys
import time
from pathlib import Path
import requests
import os

def run_cmd(cmd: str, desc: str, check: bool = True) -> bool:
    """Run command with status reporting"""
    print(f"\n🔧 {desc}")
    print(f"   Command: {cmd}")

    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, check=check
        )
        print("   ✅ Success")
        if result.stdout.strip():
            print(f"   Output: {result.stdout.strip()[:100]}...")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed: {e.stderr.strip()[:100]}")
        return False

def wait_for_service(url: str, timeout: int = 30) -> bool:
    """Wait for service to be ready"""
    print(f"⏳ Waiting for {url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"   ✅ Service ready at {url}")
                return True
        except:
            pass
        time.sleep(2)

    print(f"   ❌ Service not ready at {url} after {timeout}s")
    return False

def main():
    """Main setup automation"""
    print("🚀 E-Commerce Data Pipeline - Automated Setup")
    print("=" * 60)

    base_dir = Path(__file__).parent.parent

    # Change to project directory
    os.chdir(base_dir)

    steps = [
        # 1. Environment check
        ("python --version", "Check Python version"),
        ("uv --version", "Check uv package manager"),
        ("docker --version", "Check Docker installation"),

        # 2. Install dependencies
        ("uv sync --extra mlops", "Install Python dependencies"),

        # 3. Start infrastructure
        ("docker-compose up -d db airflow-db", "Start databases"),
        ("timeout 30 docker-compose logs db | grep 'ready'", "Wait for PostgreSQL"),
        ("docker-compose up -d airflow-webserver airflow-scheduler", "Start Airflow services"),

        # 4. Wait for services
        ("", "Wait for Airflow webserver"),  # Custom wait logic below

        # 5. Run initial ETL
        ("python src/etl_pipeline.py", "Execute ETL pipeline"),

        # 6. Start API
        ("docker-compose up -d api", "Start analytics API"),

        # 7. Run tests
        ("python -m pytest tests/ -v", "Run test suite"),
    ]

    success_count = 0

    for cmd, desc in steps:
        if cmd == "":  # Special case for Airflow wait
            if wait_for_service("http://localhost:8080"):
                success_count += 1
            continue

        if run_cmd(cmd, desc):
            success_count += 1
        else:
            print(f"❌ Setup failed at step: {desc}")
            break

    # Final validation
    print(f"\n📊 Setup Results: {success_count}/{len(steps)} steps completed")

    if success_count == len(steps):
        print("\n🎉 Setup Complete! Services running:")
        print("   📈 Airflow UI: http://localhost:8080")
        print("   🔍 API Docs: http://localhost:8000/docs")
        print("   📊 MLflow UI: mlflow ui (in separate terminal)")
        print("\n🚀 Ready for data analysis!")
    else:
        print("\n⚠️  Setup incomplete. Check logs above for failures.")
        sys.exit(1)

if __name__ == "__main__":
    main()