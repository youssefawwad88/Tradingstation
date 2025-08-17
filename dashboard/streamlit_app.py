from datetime import datetime

import streamlit as st


def load_css():
    """Injects custom CSS to style the app according to the design guidelines."""
    css = """
    <style>
        /* Main app background */
        .stApp {
            background-color: #1E1E1E; /* Charcoal/dark background */
        }

        /* Sidebar styling */
        .st-emotion-cache-16txtl3 {
            background-color: #252526;
        }

        /* Font styling */
        body, .st-emotion-cache-10trblm, .st-emotion-cache-16txtl3, .st-emotion-cache-1v0mbdj, .st-emotion-cache-1kyxreq {
            font-family: 'Inter', 'Roboto Mono', sans-serif;
            color: #FAFAFA; /* Off-white text */
        }

        /* Button styling */
        .stButton>button {
            border-radius: 8px;
            border: 1px solid #4A4A4A;
            color: #FAFAFA;
            background-color: #252526;
            transition: all 0.2s ease-in-out;
        }
        .stButton>button:hover {
            border-color: #007ACC; /* Blue accent on hover */
            color: #007ACC;
        }
        .stButton>button:focus {
            box-shadow: 0 0 0 2px #007ACC;
            outline: none;
        }

        /* Expander styling */
        .st-emotion-cache-p5msec {
            border-radius: 8px;
            border: 1px solid #4A4A4A;
            background-color: #252526;
        }

        /* Accent colors for text */
        .text-green { color: #2E7D32; }
        .text-red { color: #C62828; }
        .text-yellow { color: #F9A825; }
        .text-blue { color: #007ACC; }

    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def main():
    """
    Main function to run the Streamlit landing page.
    """
    # --- Page Configuration ---
    # This must be the first Streamlit command in your script.
    st.set_page_config(
        page_title="Trading Station Home",
        page_icon="🏠",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # --- Load Custom CSS ---
    load_css()

    # --- Page Content ---
    st.title("🏠 Welcome to Your Trading Station")
    st.markdown("---")

    st.subheader("Your automated, 24/7 trading analysis engine.")
    st.write(
        "This dashboard is your central command for monitoring the system, analyzing signals, "
        "and tracking performance. Use the navigation panel on the left to explore the different modules."
    )

    st.info(
        "**Quick Start:**\n\n"
        "1.  **`Scheduler Monitor`**: Check the real-time status of your backend data jobs.\n"
        "2.  **`System Settings`**: Manage your master ticker list and other configurations.\n"
        "3.  **`Master Screener Hub`**: View consolidated trading signals once the market is active.",
        icon="💡",
    )

    st.sidebar.success("Select a dashboard page above.")
    st.sidebar.markdown("---")
    st.sidebar.write(f"**Last Refresh:** {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
