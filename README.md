# Crypto Position Path

**Crypto Position Path** is a real‑time cryptocurrency ranking tracker that visualises the movement of trading pairs over time. 
It fetches data from multiple exchanges, ranks pairs by 24‑hour price change, and builds trajectory tracks. 
The tracks are displayed in an interactive SVG chart with pan, zoom, and detailed tooltips.


---

## ✨ Features

- **Multi‑exchange support** – Binance, MEXC, Bybit, Gate.io, KuCoin, OKX (spot & futures).
- **Quote currency filtering** – Focus on pairs quoted in USDT, BTC, ETH, etc.
- **Automatic ranking** – Pairs are ranked by 24‑h price change (1 = best gain).
- **Manual highlighting** – Mark specific pairs to always appear in the tracks.
- **Two‑point tracks** – Each track connects two consecutive snapshots; coloured by pair, dashed for manually selected pairs.
- **Interactive SVG viewer** – Pan, zoom, and hover to see detailed info (price, volume, rank change).
- **Performance logging** – Configurable log levels for different modules.
- **Data retention** – Automatic cleanup of old snapshots (configurable, e.g. 24 hours).

---

## 🛠 Technology Stack

- **Python 3.9+**
- **Streamlit** – Web UI framework
- **CCXT** – Unified cryptocurrency exchange API
- **aiohttp** – Asynchronous HTTP client
- **Pandas** – Data manipulation
- **NumPy / SciPy** – Numerical computations
- **SQLite** – Local database (snapshots, colours, settings)

---

## 🚀 Installation

### Using Conda (recommended)

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/CryptoPositionPath.git
   cd CryptoPositionPath
    ```

2. Create the environment from the provided environment.yml:
    ```bash
    conda env create -f environment.yml
     ```

3. Activate the environment:
    ```bash
    conda activate ccxt_dashboard
    ```

4. Launch the application:
    ```bash
    streamlit run main_app.py
    ```

or use the provided helper scripts:
Windows: run.bat
Linux/macOS: ./run.sh

### Using pip
1. Create and activate a virtual environment (optional).

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. Run the app:
    ```bash
     streamlit run main_app.py
    ```

## Usage
1. Configuration page – Select exchange, market type, quote currency, snapshot interval, retention period, and manually choose pairs to highlight.

2. Start data collection – Click Start; the collector runs in the background, fetching snapshots every interval seconds.

3. View tracks – Click Tracks to open the visualisation page. Use the filter panel to adjust time range, volume, rank change, and track types.

4. Interactive SVG – Pan by dragging, zoom with mouse wheel or on‑screen buttons, hover over tracks for detailed information.

5. Stop collection – Use the Stop button when finished.

6. All settings are persisted in the SQLite database (crypto_data.db).

## Project Structure
```text
crypto_dashboard/
├── run.py                     # Simple system launcher
├── main_app.py                # Main Streamlit application
├── run.bat                    # Launch script for Windows
├── run.sh                     # Launch script for Linux/macOS
├── requirements.txt           # Python dependencies
├── environment.yml            # Conda environment specification
├── logs/                      # Directory for log files
├── config_page.py             # Configuration page UI and settings management
├── data_collector.py          # Background data collection thread
├── async_fetcher.py           # Asynchronous exchange data fetcher with ranking
├── analytics_engine.py        # Builds and saves price trajectory tracks
├── data_storage.py            # SQLite database handling (snapshots, colors, settings)
├── universal_resolver.py      # Cross‑platform DNS resolver for aiohttp
├── log_viewer.py              # Performance log viewer UI
├── logger.py                  # Configurable logging (singleton)
├── svg_track_renderer.py      # SVG generation for track visualisation (pan/zoom)
├── track_builder.py           # Track data structures and DB persistence
├── manual_tracks_manager.py   # Management of manually selected tracks
├── color_manager.py           # Pair color management and display
└── crypto_data.db             # SQLite database file (created at runtime)
```


## License
This project is licensed under the MIT License – see the LICENSE file for details.