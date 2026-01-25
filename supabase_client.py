import streamlit as st
from supabase import create_client, Client

@st.cache_resource
def get_supabase_client() -> Client:
    """
    Crea y devuelve un cliente de Supabase.
    Se cachea para no crearlo de nuevo en cada recarga de Streamlit.
    """
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    supabase: Client = create_client(url, key)
    return supabase
