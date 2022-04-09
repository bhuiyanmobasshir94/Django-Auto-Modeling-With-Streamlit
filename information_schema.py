import os
import pathlib

import numpy as np
import pandas as pd
import sqlalchemy
from django.db import models
from sqlalchemy import create_engine, types
from sqlalchemy.dialects import postgresql

import streamlit as st
from django_setup import init_django

init_django('autogen')

sql_engine = create_engine("sqlite:///lite.sqlite3")

ROOT_DIR = pathlib.Path().resolve()


def row_block(field_name_key, data_type_key, field_name=None):
    col1, col2 = st.columns(2)
    with col1:
        st.text_input('Field Name', field_name, key=field_name_key)
    with col2:
        st.selectbox(
            'Data Type',
            postgresql.__all__, key=data_type_key)


with st.expander("Create data model with csv file"):
    uploaded_file = st.file_uploader(
        "Choose a CSV file", accept_multiple_files=False)
    if uploaded_file is not None:
        dataframe = pd.read_csv(uploaded_file)
        if 'row_block_count_' not in st.session_state:
            st.session_state.row_block_count_ = 0

        with st.form("data_modeling_form_1", clear_on_submit=True):
            st.text_input('Function Name', key="form_function_name_")
            for i, column in enumerate(dataframe.columns):
                st.session_state.row_block_count_ = i + 1
                field_name_key = f"field_name_{st.session_state.row_block_count_}_"
                data_type_key = f"data_type_{st.session_state.row_block_count_}_"
                row_block(field_name_key, data_type_key, column)
            submit = st.form_submit_button("Submit")
            if submit:
                datatype_dict = {}
                function_name = st.session_state["form_function_name_"]
                for i, column in enumerate(dataframe.columns):
                    field_name_key = f"field_name_{i + 1}_"
                    data_type_key = f"data_type_{i + 1}_"
                    datatype_dict[st.session_state[field_name_key]
                                  ] = getattr(types, st.session_state[data_type_key])
                dataframe = pd.DataFrame(columns=datatype_dict.keys())
                st.write(dataframe)
                dataframe.to_sql(name=function_name, if_exists='replace',
                                 con=sql_engine, dtype=datatype_dict)
                st.code(os.popen(
                    f"python manage.py inspectdb --database lite {function_name}").read(), language="python")
                os.remove(f"{ROOT_DIR}/lite.sqlite3")

with st.expander("Create data model with form"):
    nrow_block = st.sidebar.number_input("Number of row blocks", 0, 50, 1)
    if 'row_block_count' not in st.session_state:
        st.session_state.row_block_count = 0

    with st.form("data_modeling_form", clear_on_submit=True):
        st.text_input('Function Name', key="form_function_name")
        for i in range(nrow_block):
            st.session_state.row_block_count = i + 1
            field_name_key = f"field_name_{st.session_state.row_block_count}"
            data_type_key = f"data_type_{st.session_state.row_block_count}"
            row_block(field_name_key, data_type_key)
        submit = st.form_submit_button("Submit")
        if submit:
            datatype_dict = {}
            function_name = st.session_state["form_function_name"]
            for i in range(nrow_block):
                field_name_key = f"field_name_{i + 1}"
                data_type_key = f"data_type_{i + 1}"
                datatype_dict[st.session_state[field_name_key]
                              ] = getattr(types, st.session_state[data_type_key])
            dataframe = pd.DataFrame(columns=datatype_dict.keys())
            st.write(dataframe)
            dataframe.to_sql(name=function_name, if_exists='replace',
                             con=sql_engine, dtype=datatype_dict)
            st.code(os.popen(
                f"python manage.py inspectdb --database lite {function_name}").read(), language="python")
            os.remove(f"{ROOT_DIR}/lite.sqlite3")
