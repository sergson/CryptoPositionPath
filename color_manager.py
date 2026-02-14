# color_manager.py
"""
Module for managing and displaying pair colors
"""
import streamlit as st
import sqlite3
import pandas as pd
from typing import List, Dict


class ColorManager:
    """Pair colors manager"""

    def __init__(self, storage):
        self.storage = storage

    def display_colors_compact(self, limit_per_page: int = 20):
        """
        Compact display of pair colors with pagination and search
        """
        # Get all pair colors
        colors = self._get_all_colors()

        if not colors:
            st.info("No pair colors found")
            return

        st.write(f"Total pair colors: {len(colors)}")

        # Search by pair
        search_term = st.text_input("🔍 Search by pair:", key="color_search")

        if search_term:
            colors = [c for c in colors if search_term.lower() in c[0].lower()]
            st.write(f"Found: {len(colors)} pairs")

        # Pagination
        page_size = limit_per_page
        total_pages = (len(colors) + page_size - 1) // page_size

        if total_pages > 1:
            page = st.number_input(
                "Page:",
                min_value=1,
                max_value=total_pages,
                value=1,
                key="color_page"
            )
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, len(colors))
            current_colors = colors[start_idx:end_idx]
            st.write(f"Showing pairs {start_idx + 1}-{end_idx} of {len(colors)}")
        else:
            current_colors = colors

        # Create compact view using columns
        cols_per_row = 4
        colors_to_show = current_colors[:limit_per_page] if limit_per_page else current_colors

        for i in range(0, len(colors_to_show), cols_per_row):
            cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                idx = i + j
                if idx < len(colors_to_show):
                    with cols[j]:
                        pair, color = colors_to_show[idx]
                        # Compact display
                        st.markdown(f"""
                        <div style="
                            background-color: {color}; 
                            color: {'white' if self._is_dark_color(color) else 'black'};
                            padding: 8px;
                            border-radius: 4px;
                            margin: 2px;
                            font-size: 12px;
                            text-align: center;
                            border: 1px solid #ddd;
                        ">
                        <strong>{pair}</strong><br>
                        {color}
                        </div>
                        """, unsafe_allow_html=True)

        # Button to export all colors
        if st.button("📥 Export all colors"):
            df = pd.DataFrame(colors, columns=['Pair', 'Color'])
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="pair_colors.csv",
                mime="text/csv"
            )

    def _get_all_colors(self) -> List:
        """Get all pair colors"""
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT pair, color FROM pair_colors WHERE is_system = 0 ORDER BY pair')
            return cursor.fetchall()
        finally:
            conn.close()

    def _is_dark_color(self, hex_color: str) -> bool:
        """Determine if color is dark"""
        if hex_color.startswith('#'):
            hex_color = hex_color[1:]

        if len(hex_color) == 3:
            hex_color = ''.join([c * 2 for c in hex_color])

        try:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)

            # Luminance perception formula
            brightness = (0.299 * r + 0.587 * g + 0.114 * b) / 255
            return brightness < 0.5
        except:
            return False