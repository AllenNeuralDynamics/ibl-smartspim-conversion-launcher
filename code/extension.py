import concurrent
import concurrent.futures
import contextlib
import dataclasses
import datetime
import json
import logging
import time
from collections.abc import Mapping
from typing import Any, Container, Iterable

import aind_session
import aind_session.extensions
import aind_session.utils
import aind_session.utils.codeocean_utils
import aind_session.utils.misc_utils
import codeocean.computation
import codeocean.data_asset
import npc_io
import npc_session
import pandas as pd
import upath
from typing_extensions import Self

logger = logging.getLogger(__name__)

SCRATCH_STORAGE_DIR = upath.UPath(
    "s3://aind-scratch-data/ben.hardcastle/ibl_annotation_test"
)  #! temp location for testing


class NeuroglancerState:

    content: Mapping[str, Any]

    def __init__(
        self, path_or_dict: npc_io.PathLike | Mapping[str, Any] | Self
    ) -> None:
        self._session = None
        if isinstance(path_or_dict, NeuroglancerState):
            self._session = path_or_dict._session
            self.content = path_or_dict.content
        elif isinstance(path_or_dict, Mapping):
            self.content = path_or_dict  # we won't mutate, so no need to copy
        else:
            self.content = json.loads(npc_io.from_pathlike(path_or_dict).read_text())

    def __repr__(self):
        try:
            return f"{self.__class__.__name__}({self.session.id})"
        except ValueError:
            return f"{self.__class__.__name__}({list(self.content.keys())})"

    @property
    def image_sources(self) -> tuple[str, ...]:
        with contextlib.suppress(KeyError):
            return tuple(
                (
                    layer["source"]
                    if isinstance(layer["source"], str)
                    else layer["source"]["url"]
                )
                for layer in self.content["layers"]
                if layer["type"] == "image"
            )
        return ()

    @property
    def session(self) -> aind_session.Session:
        session_ids = set()
        if self._session is None:
            for source in self.image_sources:
                try:
                    session_ids.add(npc_session.AINDSessionRecord(source))
                except ValueError:
                    continue
            if not session_ids:
                raise ValueError(
                    "No session ID could be extracted from Neuroglancer state json (expected to extract SmartSPIM session ID from image source)"
                )
            if len(session_ids) > 1:
                raise NotImplementedError(
                    f"Cannot currently handle Neuroglancer state json from multiple image sources: {session_ids}"
                )
            self._session = aind_session.Session(session_ids.pop())  # type: ignore[assignment]
        assert self._session is not None
        return self._session

    @property
    def subject_id(self) -> str:
        return str(self.session.subject_id)

    @property
    def annotation_names(self) -> tuple[str, ...]:
        names = []
        with contextlib.suppress(KeyError):
            for layer in self.content["layers"]:
                if layer["type"] != "annotation":
                    continue
                names.append(layer["name"])
        return tuple(names)

    @staticmethod
    def get_new_state_json_name(session_id: str) -> str:
        return f"{session_id}_neuroglancer-state_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.json"

    def write(
        self, path: npc_io.PathLike | None = None, timeout_sec: float = 10
    ) -> upath.UPath:
        if path is not None:
            path = npc_io.from_pathlike(path)
        else:
            path = (
                NeuroglancerExtension.storage_dir
                / NeuroglancerState.get_new_state_json_name(self.session.id)
            )
        logger.debug(f"Writing Neuroglancer annotation file to {path.as_posix()}")
        path.write_text(json.dumps(self.content, indent=2))
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if path.exists():
                break
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Failed to write Neuroglancer annotation file to {path.as_posix()}: "
                f"file not found after {timeout_sec} seconds"
            )
        logger.debug(f"Neuroglancer annotation file written to {path.as_posix()}")
        return path

    def create_data_asset(self) -> codeocean.data_asset.DataAsset:
        path = self.write()
        asset_params = codeocean.data_asset.DataAssetParams(
            name=path.stem,
            mount=path.stem,
            tags=["neuroglancer", "ecephys", "annotation", self.session.subject.id],
            source=codeocean.data_asset.Source(
                aws=codeocean.data_asset.AWSS3Source(
                    bucket=(bucket := path.as_posix().split("/")[2]),
                    prefix=(path.as_posix().split(bucket)[1].strip("/")),
                    keep_on_external_storage=False,
                    public=False,
                )
            ),
        )
        logger.debug(f"Creating asset {asset_params.name}")
        asset = aind_session.utils.codeocean_utils.get_codeocean_client().data_assets.create_data_asset(
            asset_params
        )
        logger.debug(f"Waiting for new asset {asset.name} to be ready")
        updated_asset = aind_session.utils.codeocean_utils.get_codeocean_client().data_assets.wait_until_ready(
            data_asset=asset,
            timeout=60,
        )
        logger.debug(f"Asset {updated_asset.name} is ready")
        return updated_asset


@aind_session.register_namespace(name="ibl_data_converter", cls=aind_session.Subject)
class IBLDataConverterExtension(aind_session.ExtensionBaseClass):

    _base: aind_session.Subject
    storage_dir = SCRATCH_STORAGE_DIR

    DATA_CONVERTER_CAPSULE_ID = "372263e6-d942-4241-ba71-763a1062f2b7"  #! test capsule
    # TODO switch to actual capsule: "d4ba01c4-5665-4163-95d2-e481f4465b86"
    """https://codeocean.allenneuraldynamics.org/capsule/1376129/tree"""

    @property
    def ecephys_sessions(self) -> tuple[aind_session.Session, ...]:
        return tuple(
            sorted(
                session
                for session in self._base.sessions
                if session.platform == "ecephys"
            )
        )

    @property
    def ecephys_data_assets(self) -> tuple[codeocean.data_asset.DataAsset, ...]:
        assets = []
        for session in self.ecephys_sessions:
            if not (asset := session.raw_data_asset):
                logger.warning(
                    f"{session.id} raw data has not been uploaded: cannot use for annotation"
                )
                continue
            assets.append(asset)
            logger.debug(f"Using {asset.name} for annotation")
        return aind_session.utils.codeocean_utils.sort_by_created(assets)

    @property
    def sorted_data_assets(
        self,
    ) -> tuple[aind_session.extensions.ecephys.SortedDataAsset, ...]:
        assets_all_sessions = []

        def get_session_assets(
            session: aind_session.Session,
        ) -> tuple[aind_session.extensions.ecephys.SortedDataAsset, ...]:
            return tuple(
                a
                for a in session.ecephys.sorted_data_assets
                if not a.is_sorting_error
                # and not a.is_sorting_analyzer #TODO are both supported?
            )

        future_to_session: dict[concurrent.futures.Future, aind_session.Session] = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for session in self._base.sessions:
                if session.platform != "ecephys":
                    continue
                future = executor.submit(get_session_assets, session)
                future_to_session[future] = session
            for future in concurrent.futures.as_completed(future_to_session):
                session = future_to_session[future]
                assets_this_session = future.result()
                if not assets_this_session:
                    logger.warning(
                        f"{session.id} has no sorted data (in a non-errored state): cannot use for annotation"
                    )
                    continue
                assets_all_sessions.extend(assets_this_session)
        return aind_session.utils.codeocean_utils.sort_by_created(assets_all_sessions)

    @property
    def smartspim_data_assets(self) -> tuple[codeocean.data_asset.DataAsset, ...]:
        assets = []
        for session in self._base.sessions:
            if session.platform != "SmartSPIM":
                continue
            if not hasattr(session, "raw_data_asset"):
                logger.warning(f"{session.id} has no raw data asset")
                continue
            assets.append(session.raw_data_asset)
            logger.debug(f"Found asset {session.raw_data_asset.name!r}")
        # if not assets:
        #     raise AttributeError(f"No SmartSPIM data asset found for {self._base.id}")
        # if len(assets) > 1:
        #     logger.info(
        #         f"Multiple SmartSPIM raw data assets found for {self._base.id}: using most-recent"
        #     )
        return aind_session.utils.codeocean_utils.sort_by_created(assets)  # [-1]

    @dataclasses.dataclass
    class ManifestRecord:
        mouseid: str
        probe_id: str  # can't be found automatically
        probe_name: str
        sorted_recording: str
        probe_file: str
        surface_finding: int | None = None  # not currently used
        annotation_format: str = "json"

    @property
    def csv_manifest_path(self) -> upath.UPath:
        return (
            self.storage_dir
            / "manifests"
            / f"{self._base.id}_data_converter_manifest.csv"
        )

    def create_manifest_asset(
        self,
        completed_df: pd.DataFrame,
        asset_name: str | None = None,
        skip_existing: bool = True,
        timeout_sec: float = 10,
    ) -> codeocean.data_asset.DataAsset:
        if skip_existing and (existing := getattr(self, "manifest_data_asset", None)):
            logger.info(
                f"Manifest asset already exists for {self._base.id}. Use `self.create_manifest_asset(skip_existing=False)` to force creation"
            )
            return existing
        logger.debug(f"Writing annotation manifest to {self.csv_manifest_path}")
        with self.csv_manifest_path.open("w") as f:
            completed_df.to_csv(f, index=False)
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if self.csv_manifest_path.exists():
                break
            time.sleep(1)
        else:
            raise TimeoutError(
                f"Failed to write annotation manifest to {self.csv_manifest_path}: "
                f"file not found after {timeout_sec} seconds"
            )
        asset_params = codeocean.data_asset.DataAssetParams(
            name=asset_name or self.csv_manifest_path.stem,
            mount=asset_name or self.csv_manifest_path.stem,
            tags=["ibl", "annotation", "manifest", self._base.id],
            source=codeocean.data_asset.Source(
                aws=codeocean.data_asset.AWSS3Source(
                    bucket=(bucket := self.csv_manifest_path.as_posix().split("/")[2]),
                    prefix=(
                        self.csv_manifest_path.as_posix().split(bucket)[1].strip("/")
                    ),
                    keep_on_external_storage=False,
                    public=False,
                )
            ),
        )
        logger.debug(f"Creating asset {asset_params.name}")
        asset = aind_session.utils.codeocean_utils.get_codeocean_client().data_assets.create_data_asset(
            asset_params
        )
        logger.debug(f"Waiting for new asset {asset.name} to be ready")
        updated_asset = aind_session.utils.codeocean_utils.get_codeocean_client().data_assets.wait_until_ready(
            data_asset=asset,
            timeout=60,
        )
        logger.debug(f"Asset {updated_asset.name} is ready")
        return updated_asset

    @property
    def manifest_data_asset(self) -> codeocean.data_asset.DataAsset:
        try:
            assets = aind_session.utils.codeocean_utils.get_data_assets(
                self.csv_manifest_path.stem,
                ttl_hash=aind_session.utils.misc_utils.get_ttl_hash(seconds=1),
            )
        except ValueError:
            assets = ()
        if not assets:
            raise AttributeError(
                f"No manifest asset has been created yet for {self._base.id}: run `self.create_manifest_asset()`"
            )
        if len(assets) > 1:
            logger.debug(
                f"Multiple manifest assets found for {self._base.id}: using most-recent"
            )
        return assets[-1]

    def run_data_converter_capsule(self) -> codeocean.computation.Computation:
        run_params = codeocean.computation.RunParams(
            capsule_id=self.DATA_CONVERTER_CAPSULE_ID,
            data_assets=[
                codeocean.computation.DataAssetsRunParam(id=asset.id, mount=asset.name)
                for asset in (
                    *self.ecephys_data_assets,
                    *self.sorted_data_assets,
                    self.smartspim_data_assets[-1],
                    self.manifest_data_asset,
                )
            ],
            parameters=[],
            named_parameters=[],
        )
        logger.debug(f"Running data converter capsule: {run_params.capsule_id}")
        return aind_session.utils.codeocean_utils.get_codeocean_client().computations.run_capsule(
            run_params
        )

    def get_ecephys_sessions(
        self,
        ecephys_data_asset_names: Container[str] = (),
    ) -> tuple[aind_session.Session, ...]:
        return tuple(
            session
            for session in self._base.sessions
            if session.platform == "ecephys"
            and (
                session.raw_data_asset.name in ecephys_data_asset_names
                if ecephys_data_asset_names
                else True
            )
        )

    def get_sorted_data_assets(
        self,
        sorted_data_asset_names: Container[str] = (),
    ) -> tuple[aind_session.extensions.ecephys.SortedDataAsset, ...]:
        results = tuple(
            asset
            for session in self._base.sessions
            if session.platform == "ecephys"
            for asset in session.ecephys.sorter.kilosort2_5.sorted_data_assets
            if (
                asset.name in sorted_data_asset_names
                if sorted_data_asset_names
                else True
            )
        )
        for name in sorted_data_asset_names:
            if not any(asset.name == name for asset in results):
                logger.warning(f"Requested sorted data asset {name} not found")
        return results

    def get_partial_df(
        self,
        neuroglancer_state_json_name: str | None = None,
        sorted_data_asset_names: Iterable[str] = (),
    ) -> pd.DataFrame:
        ng: NeuroglancerExtension = self._base.neuroglancer  # type: ignore[attr-defined]
        if not neuroglancer_state_json_name:
            try:
                latest = ng.state_json_paths[-1]
            except IndexError:
                raise FileNotFoundError(
                    f"No Neuroglancer annotation json found for {self._base.id} in {ng.storage_dir}"
                )
            logger.debug(
                f"Using most-recent Neuroglancer annotation file: {latest.as_posix()}"
            )
            neuroglancer_state_json_name = latest.stem
            neuroglancer_state = NeuroglancerState(latest)
        else:
            neuroglancer_state = NeuroglancerState(
                ng.storage_dir / f"{neuroglancer_state_json_name}.json"
            )

        if isinstance(sorted_data_asset_names, str):
            sorted_data_asset_names = (sorted_data_asset_names,)
        if not sorted_data_asset_names:
            sorted_data_asset_names = sorted(
                asset.name for asset in self.sorted_data_assets
            )

        records = []
        for annotation_name in neuroglancer_state.annotation_names:
            for sorted_data_asset_name in sorted_data_asset_names:
                row = IBLDataConverterExtension.ManifestRecord(
                    mouseid=self._base.id,
                    probe_name="",
                    probe_id=annotation_name,
                    sorted_recording=sorted_data_asset_name,
                    probe_file=neuroglancer_state_json_name,
                )
                records.append(row)
        return pd.DataFrame.from_records(
            [dataclasses.asdict(record) for record in records]
        )


@aind_session.register_namespace(name="neuroglancer", cls=aind_session.Subject)
class NeuroglancerExtension(aind_session.extension.ExtensionBaseClass):

    storage_dir = SCRATCH_STORAGE_DIR / "neuroglancer_states"
    _base: aind_session.Subject

    @property
    def state_json_paths(self) -> tuple[upath.UPath, ...]:
        return tuple(
            sorted(
                self.storage_dir.glob(f"*{self._base.id}_*.json"), key=lambda p: p.stem
            )
        )

    @property
    def states(
        self,
    ) -> tuple[NeuroglancerState, ...]:
        return tuple(NeuroglancerState(p) for p in self.state_json_paths)

    # @property
    # def neuroglancer(self) -> NeuroglancerExtension:
    #     return self.NeuroglancerExtension(self._base)


