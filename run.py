#!/usr/bin/env python3
#run.py
"""
Simple launch script for Crypto Pixel Tracker
"""
import subprocess
import sys


def main():
    """Run the Streamlit application"""
    print("=" * 50)
    print("🚀 Crypto Position Path")
    print("=" * 50)
    print("📊 Interface: http://localhost:8501")
    print("=" * 50)
    print("\nPress Ctrl+C to stop\n")

    # Path to the main application
    streamlit_app = "main_app.py"

    # Command to run Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        streamlit_app,
        "--server.port", "8501",
        "--server.address", "localhost",
        "--server.headless", "false",
        "--browser.serverAddress", "localhost",
        "--browser.gatherUsageStats", "false",
        "--theme.base", "dark"
    ]

    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n🛑 Application stopped")
    except Exception as e:
        print(f"❌ Launch error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())