"""
- get existing json paths, or allow user to paste json
- display json in a json viewer
- allow user to select from available smartspim assets, then sorted data assets (with probe names
  and sorter version)
- display partial df
- button linked to writing assets and launching the pipeline
"""

import concurrent.futures
import json
import logging

import pandas as pd
import streamlit as st
import streamlit.logger

from extension import (
    IBLDataConverterExtension,
    NeuroglancerExtension,
    NeuroglancerState,
)

logging.basicConfig(level=logging.INFO)

logger = streamlit.logger.get_logger(__name__)


def get_existing_json_paths():
    paths = sorted(
        NeuroglancerExtension.state_json_dir.rglob("*.json"),
        key=lambda p: "_".join(p.stem.split("_")[-2:]),
        reverse=True,
    )
    logger.info(
        f"Found {len(paths)} existing json files in {NeuroglancerExtension.state_json_dir}"
    )
    return paths


existing_paths = get_existing_json_paths()

st.session_state.setdefault("ng_state", None)
st.session_state.setdefault("ng_path", None)


def update_ng_state(source: str) -> None:
    if source == "selectbox":
        path = st.session_state["selectbox"]
        logger.info(f"Creating new NeuroglancerState from selectbox path: {path}")
        st.session_state["ng_state"] = NeuroglancerState(path.read_text())
        st.session_state["ng_path"] = path
    elif source == "text_input":
        logger.info("Creating new NeuroglancerState from text input")
        st.session_state["ng_state"] = state = NeuroglancerState(
            st.session_state["text_input"]
        )
        st.session_state["ng_path"] = state.write()
    else:
        raise ValueError(f"Invalid source: {source}")


st.title("Get Neuroglancer state json")
ng_path = st.selectbox(
    "Use an existing file:",
    options=existing_paths,
    index=None,
    format_func=lambda p: p.stem,
    placeholder="Type to search...",
    key="selectbox",
    on_change=update_ng_state,
    kwargs={"source": "selectbox"},
)

user_input = st.text_input(
    "Or create a new file:",
    key="text_input",
    placeholder="Paste json and hit Enter...",
    on_change=update_ng_state,
    kwargs={"source": "text_input"},
)

if st.session_state["ng_path"] is not None:
    st.json(json.loads(st.session_state["ng_path"].read_text()), expanded=1)

    state = st.session_state["ng_state"]
    ibl_data_converter: IBLDataConverterExtension = (
        state.session.subject.ibl_data_converter
    )

    # TODO allow user to select from available smartspim assets

    def get_sorted_asset_df() -> pd.DataFrame:
        if state is None:
            return pd.DataFrame()

        def helper(asset):
            return {
                "name": asset.name,
                "probes": list(asset.sorted_probes),
                "sorter": asset.sorter_name,
                "is_analyzer": asset.is_sorting_analyzer,
                "is_error": asset.is_sorting_error,
                "id": asset.id,
            }

        with concurrent.futures.ThreadPoolExecutor() as executor:
            records = executor.map(helper, ibl_data_converter.sorted_data_assets)
        return pd.DataFrame.from_records(records)

    st.title("Select sorted data assets to use")
    st.info(
        "Remove unwanted sorted data assets by selecting rows (left) and using the trash can (right)"
    )
    sorted_asset_df = st.data_editor(
        get_sorted_asset_df(),
        num_rows="dynamic",
        hide_index=True,
        disabled=["name", "probes", "sorter", "is_analyzer", "is_error", "id"],
    )

    def get_manifest_df() -> pd.DataFrame:
        sorted_data_asset_names = sorted_asset_df["name"].tolist()
        return (
            pd.DataFrame(
                ibl_data_converter.get_partial_manifest_records(
                    sorted_data_asset_names=sorted_data_asset_names,
                    neuroglancer_state_json_name=st.session_state["ng_path"].stem,
                )
            )
            .sort_values(["sorted_recording", "probe_id"])
            .reset_index()
        )

    st.title("Create manifest file")
    st.info(
        "Fill out the `probe_name` column with names from Open Ephys and remove any unwanted rows"
    )
    manifest_df = st.data_editor(
        get_manifest_df(),
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "mouseid": st.column_config.TextColumn(width="small"),
            "sorted_recording": st.column_config.TextColumn(width="large"),
            "probe_name": st.column_config.SelectboxColumn(
                width="small",
                options={
                    probe for sublist in sorted_asset_df["probes"] for probe in sublist
                },
            ),
            "probe_file": st.column_config.TextColumn(width="large"),
            "probe_id": st.column_config.TextColumn(width="small"),
            "surface_finding": st.column_config.TextColumn(width="large"),
            "annotation_format": st.column_config.SelectboxColumn(
                options=["json", "swc"]
            ),
        },
    )

    if st.button("Launch data converter", type="primary"):

        asset = ibl_data_converter.create_manifest_asset(
            manifest_df.to_dict(orient="records"),
            skip_existing=False,
            timeout_sec=30,
        )
        assert asset.id == ibl_data_converter.manifest_data_asset.id

        capsule_id = ibl_data_converter.DATA_CONVERTER_CAPSULE_ID
        computation = ibl_data_converter.run_data_converter_capsule(
            capsule_id=capsule_id,
            additional_assets=[asset],
        )
        st.success(f"Launched data converter capsule {capsule_id!r}")