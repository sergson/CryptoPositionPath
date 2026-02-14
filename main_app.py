#main_app.py
"""
Main Streamlit application
"""
import streamlit as st
from config_page import ConfigPage
from data_storage import DataStorage
from log_viewer import LogViewer
from logger import perf_logger


def main():
    """Main application function"""

    # Page configuration
    st.set_page_config(
        page_title="Crypto Position Path",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Initialize session state
    if "page" not in st.session_state:
        st.session_state.page = "config"

    if "storage" not in st.session_state:
        st.session_state.storage = DataStorage()
        perf_logger.initialize_with_storage(st.session_state.storage)

    # Page navigation
    if st.session_state.page == "config":
        # Configuration page
        config_page = ConfigPage()
        config_page.display()

    elif st.session_state.page == "logs":
        # Logs page
        log_viewer = LogViewer()
        log_viewer.display()

    elif st.session_state.page == "tracks":
        # Tracks display page
        if "config" not in st.session_state:
            st.error("Configuration not found.")
            if st.button("← Settings"):
                st.session_state.page = "config"
                st.rerun()
            return

        config = st.session_state.config

        # Header
        col1, col2 = st.columns([4, 1])
        with col1:
            st.title(f"📈 {config['exchange'].upper()} - Trajectory Tracks")
        with col2:
            if st.button("← Settings"):
                st.session_state.page = "config"
                st.rerun()

        # Display tracks
        try:
            from svg_track_renderer import SVGTrackRenderer
            renderer = SVGTrackRenderer(st.session_state.storage)
            renderer.display_tracks_in_streamlit(
                config["exchange"],
                config["markets"][0] if config["markets"] else "spot"
            )
        except ImportError as e:
            st.error(f"Track rendering module not available: {e}")
            if st.button("Install dependencies"):
                st.code("pip install scipy numpy")
        except Exception as e:
            st.error(f"Error displaying tracks: {e}")
            st.code(str(e))


if __name__ == "__main__":
    main()