#log_viewer.py
"""
Module for viewing and analyzing performance logs
"""
import streamlit as st
import os
import pandas as pd
from datetime import datetime, timedelta
import re


class LogViewer:
    """Performance log viewer"""

    def __init__(self):
        self.log_dir = "logs"

    def display(self):
        """Display the log viewer interface"""
        st.title("📊 Logs")

        if st.button("← Settings"):
            st.session_state.page = "config"
            st.rerun()

        # Select log file
        log_files = self.get_log_files()

        if not log_files:
            st.warning("No log files found")
            return

        selected_file = st.selectbox("Choose log file:", log_files)

        # Analysis parameters
        col1, col2 = st.columns(2)

        with col1:
            show_lines = st.slider("Number of lines", 10, 5000, 100)
            filter_level = st.multiselect(
                "Filter by level",
                ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                default=["WARNING", "ERROR", "CRITICAL"]
            )

        with col2:
            search_term = st.text_input("Search by text", "")
            time_filter = st.selectbox(
                "Time filter",
                ["All", "Last hour", "Today", "Last 24 hours"]
            )

        # Performance analysis
        if st.button("📈 Performance logs", key="analyze_perf"):
            self.analyze_performance(selected_file)

        # View logs
        if st.button("🔍 Show logs", key="show_logs"):
            self.show_logs(selected_file, show_lines, filter_level, search_term, time_filter)

    def get_log_files(self):
        """Get list of log files"""
        if not os.path.exists(self.log_dir):
            return []

        files = []
        for file in os.listdir(self.log_dir):
            if file.endswith('.log'):
                file_path = os.path.join(self.log_dir, file)
                file_size = os.path.getsize(file_path) / 1024  # Size in KB
                files.append({
                    'name': file,
                    'path': file_path,
                    'size_kb': f"{file_size:.1f}",
                    'modified': datetime.fromtimestamp(os.path.getmtime(file_path))
                })

        # Sort by modification date
        files.sort(key=lambda x: x['modified'], reverse=True)
        return [f"{f['name']} ({f['size_kb']} KB, {f['modified'].strftime('%m/%d %H:%M')})"
                for f in files]

    def show_logs(self, file_display, lines_count, filter_level, search_term, time_filter):
        """Display logs"""
        # Extract file name from display string
        file_name = file_display.split(' (')[0]
        file_path = os.path.join(self.log_dir, file_name)

        if not os.path.exists(file_path):
            st.error("File not found")
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()

            # Filtering
            filtered_lines = []
            for line in all_lines:
                if not line.strip():
                    continue

                # Filter by level
                if filter_level:
                    level_match = re.search(r'\[(\w+)\s*\]', line)
                    if level_match:
                        level = level_match.group(1).strip()
                        if level not in filter_level:
                            continue

                # Filter by search term
                if search_term and search_term.lower() not in line.lower():
                    continue

                # Filter by time
                if time_filter != "All":
                    try:
                        time_str = line.split(' ')[0]
                        log_time = datetime.strptime(time_str, '%H:%M:%S')
                        now = datetime.now()

                        if time_filter == "Last hour":
                            if (now - log_time).total_seconds() > 3600:
                                continue
                        elif time_filter == "Today":
                            # All logs for today (relative to log time)
                            # For simplicity, take all lines
                            pass
                    except:
                        pass

                filtered_lines.append(line)

            # Take last N lines
            display_lines = filtered_lines[-lines_count:] if lines_count else filtered_lines

            st.text_area("Logs:", "".join(display_lines), height=500)

            # Statistics
            st.info(f"Total lines: {len(all_lines)}, Filtered: {len(display_lines)}")

        except Exception as e:
            st.error(f"Error reading file: {e}")

    def analyze_performance(self, file_display):
        """Performance analysis from logs"""
        file_name = file_display.split(' (')[0]
        file_path = os.path.join(self.log_dir, file_name)

        if not os.path.exists(file_path):
            st.error("File not found")
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            # Extract method execution times
            time_pattern = r'(\w+) took (\d+\.\d+) sec'
            method_times = {}

            for line in lines:
                match = re.search(time_pattern, line)
                if match:
                    method = match.group(1)
                    time_taken = float(match.group(2))

                    if method not in method_times:
                        method_times[method] = {
                            'count': 0,
                            'total_time': 0,
                            'max_time': 0,
                            'min_time': float('inf'),
                            'calls': []
                        }

                    method_times[method]['count'] += 1
                    method_times[method]['total_time'] += time_taken
                    method_times[method]['max_time'] = max(method_times[method]['max_time'], time_taken)
                    method_times[method]['min_time'] = min(method_times[method]['min_time'], time_taken)
                    method_times[method]['calls'].append(time_taken)

            # Create DataFrame for display
            if method_times:
                data = []
                for method, stats in method_times.items():
                    avg_time = stats['total_time'] / stats['count'] if stats['count'] > 0 else 0
                    data.append({
                        'Method': method,
                        'Calls': stats['count'],
                        'Total time': f"{stats['total_time']:.3f} sec",
                        'Average time': f"{avg_time:.3f} sec",
                        'Maximum': f"{stats['max_time']:.3f} sec",
                        'Minimum': f"{stats['min_time']:.3f} sec"
                    })

                df = pd.DataFrame(data)
                df = df.sort_values('Total time', ascending=False)

                st.subheader("📈 Method performance statistics")
                st.dataframe(df, use_container_width=True)

                # Visualization
                if not df.empty:
                    st.subheader("📊 Average execution time chart")

                    # Take top 10 methods
                    top_methods = df.head(10).copy()
                    top_methods['Average time num'] = top_methods['Average time'].str.replace(' sec', '').astype(
                        float)

                    st.bar_chart(top_methods.set_index('Method')['Average time num'])
            else:
                st.info("No performance data found in logs")

        except Exception as e:
            st.error(f"Analysis error: {e}")