# aind-session-demo
Example usage of
[`aind-session`](https://github.com/AllenNeuralDynamics/aind-session), a Python
package wrapping the DocDB client and CodeOcean API client, providing convenient
access to paths, metadata and assets related to AIND sessions (experiments).

# Usage

## User secrets
Credentials are required for:
  - AWS
    - use the `AWS Assumable Role - aind-codeocean-user` secret
  - CodeOcean API
    - an access token is required with at least `Datasets: Read` scope (see
      [CodeOcean
      docs](https://docs.codeocean.com/user-guide/code-ocean-api/authentication)
      on how to create one)
    - in a capsule, this can be found under the `API credentials` secret

## Python
```python
>>> import aind_session

# Common attributes available for all sessions:
>>> session = aind_session.Session('ecephys_676909_2023-12-13_13-43-40')
>>> session.platform
'ecephys'
>>> session.subject_id
'676909'
>>> session.dt
datetime.datetime(2023, 12, 13, 13, 43, 40)
>>> len(session.data_assets)            # doctest: +SKIP
42
>>> session.is_uploaded
True
>>> session.raw_data_asset.id
'16d46411-540a-4122-b47f-8cb2a15d593a'
>>> session.raw_data_dir.as_posix()
's3://aind-ephys-data/ecephys_676909_2023-12-13_13-43-40'
>>> session.modalities
('behavior', 'behavior_videos', 'ecephys')
>>> session.docdb.keys()
dict_keys(['_id', 'acquisition', 'created', 'data_description', 'describedBy', 'external_links', 'instrument', 'last_modified', 'location', 'metadata_status', 'name', 'procedures', 'processing', 'rig', 'schema_version', 'session', 'subject'])
>>> session.docdb['subject']['genotype']
'Pvalb-IRES-Cre/wt;Ai32(RCL-ChR2(H134R)_EYFP)/wt'

# Additional functionality is provided in namespace extensions:
>>> session.ecephys.is_sorted
True
>>> session.ecephys.sorter.kilosort2_5.data_assets[-1].name
'ecephys_676909_2023-12-13_13-43-40_sorted_2024-03-01_16-02-45'

# Objects refer to the original session, regardless of how they were created:
>>> a = aind_session.Session('ecephys_676909_2023-12-13_13-43-40')
>>> b = aind_session.Session('ecephys_676909_2023-12-13_13-43-40_sorted_2024-03-01_16-02-45')
>>> assert a == b, "Objects are equal if they refer to the same session ID"

# Objects are also hashable and sortable (by their ID)
```

Search for session objects by subject ID, platform, date:
```python
>>> import aind_session

>>> sessions: tuple[aind_session.Session, ...] = aind_session.get_sessions(subject_id=676909)
>>> sessions[0].platform
'behavior'
>>> sessions[0].date
'2023-10-24'

# Filter sessions by platform:
>>> aind_session.get_sessions(subject_id=676909, platform='ecephys')[0].platform
'ecephys'

# Filter sessions by date (most common formats accepted):
>>> a = aind_session.get_sessions(subject_id=676909, date='2023-12-13')
>>> b = aind_session.get_sessions(subject_id=676909, date='2023-12-13_13-43-40')
>>> c = aind_session.get_sessions(subject_id=676909, date='2023-12-13 13:43:40')
>>> d = aind_session.get_sessions(subject_id=676909, date='20231213')
>>> e = aind_session.get_sessions(subject_id=676909, date='20231213_134340')
>>> a == b == c == d == e
True

# Filter sessions by start or end date (can be open on either side):
>>> aind_session.get_sessions(subject_id=676909, start_date='2023-12-13')
(Session('ecephys_676909_2023-12-13_13-43-40'), Session('ecephys_676909_2023-12-14_12-43-11'))
>>> aind_session.get_sessions(subject_id=676909, start_date='2023-12-13', end_date='2023-12-14_10-00-00')
(Session('ecephys_676909_2023-12-13_13-43-40'),)

```