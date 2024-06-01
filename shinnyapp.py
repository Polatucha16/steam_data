# -*- coding: utf-8 -*-
"""
Created on Fri Feb  2 18:23:45 2024

@author: HugoN
"""

import streamlit as st
import pandas as pd

def main():
    
    st.set_page_config(layout="centered")
    title = "Steam playerbase similarity"
    tooltip = "Results might be missrepresented for newer or less popular games."
    
    st.markdown("""
        <h1 title="Results might be missrepresented for newer or less popular games." style="cursor: help;">Steam playerbase similarity<span style="color: blue; font-size: 38px;">(?)</span></h1>
    """, unsafe_allow_html=True)
    
    tooltip_text = "This is the data table."
    tooltip_css = "font-size: 24px; cursor: help;"
    tooltip_markdown = f'<span title="{tooltip_text}" style="{tooltip_css}">Data Table <span style="color: blue; font-size: 18px;">(?)</span></span>'
    #st.markdown(tooltip_markdown, unsafe_allow_html=True)

    data_file = "data\part_sim_rdbt.csv"
    df = pd.read_csv(data_file)

    # search_term = st.text_input("Search by game1:")
    unique_names = df.game1.unique()
    search_term = st.selectbox("start input here:", options=unique_names)
    # st.write('You selected:', option)
    if search_term:
        #filtered_df = df[df['game1'].str.contains(search_term, case=False)]
        filtered_df = df[df['game1']==search_term]
        filtered_df = filtered_df.sort_values('score', ascending=False)
    else:
        filtered_df = df

    filtered_df.index = range(len(filtered_df))
    
    
    st.latex(r'\text{Similarity score = } \frac{\text{hours}(\text{playerbase1} \cap \text{playerbase2})}{\text{hours}(\text{playerbase1} \cup \text{playerbase2})}')
    
    st.dataframe(filtered_df, height=550, width=1000)

if __name__ == "__main__":
    main()