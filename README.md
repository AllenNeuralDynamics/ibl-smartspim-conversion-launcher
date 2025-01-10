# ibl-smartspim-conversion-launcher
A Streamlit app for gathering assets and mapping probe tracks in Neuroglancer to probe recordings in Open Ephys. The app then launches a data conversion capsule and creates a data asset required to use the IBL annotation app.

# Usage
Credentials first need to be added to **two capsules**: this one, and a "monitor" capsule that handles data asset creation:

- when the `launch data converter` button is pressed at the end of the Streamlit app, a request is made via the CodeOcean API to a "monitor" capsule
- the monitor capsule starts the data cpnversion capsule (forwarding the run parameters sent from the Streamlit app)
- the monitor capsule waits for data conversion to finish (up to 3 hours) then creates a data asset from the results 
- this way, the Streamlit app can be closed or used to launch conversion for another session

Credentials need to be added at the bottom of each capsule's `environment` page.
  - AWS
    - use the `AWS Assumable Role - aind-codeocean-user` secret
  - CodeOcean API
    - an access token is required with read and write scope on capsules and datasets (see
      [CodeOcean
      docs](https://docs.codeocean.com/user-guide/code-ocean-api/authentication)
      on how to create one)

---
*Note: These capsules should be used directly, not duplicated*

- Monitor capsule: https://codeocean.allenneuraldynamics.org/capsule/5449547/tree
- Data conversion capsule: https://codeocean.allenneuraldynamics.org/capsule/8363069/tree

---

## Launch Streamlit

After adding credentials, launch the Streamlit cloud workstation (red origami icon, under `Reproducible Run`).

Various functions in `code/extension.py` are used to parse json exported from Neuroglancer, then find corresponding data assets for ecephys, spike-sorting and smartspim.

Carefully check the auto-populated tables at each stage, and remove or add lines as necessary.

It's possible to copy/paste cells from the tables, allowing you to work in Excel if you prefer.