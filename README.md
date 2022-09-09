# Retail Demo 

[Programmatically Manage and Create Multiple Live Tables](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cookbook.html#programmatically-manage-and-create-multiple-live-tables)



Here is a sample of my configuration for the Delta Live Table pipeline. Please note that the `target` is the target schema/database you want to register your tables to. This value needs to be supplied as a widget in the `ViewData` notebook.  
```json
{
    "id": "AUTO GENERATED ID",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "LEGACY"
            }
        }
    ],
    "development": true,
    "continuous": true,
    "channel": "CURRENT",
    "edition": "ADVANCED",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/ryan.chynoweth@databricks.com/DemoContent/delta_demos/DLT/dynamic_dlt/DLT_Pipeline"
            }
        }
    ],
    "name": "rac_dynamic_dlt",
    "storage": "dbfs:/pipelines/007538bf-a382-41b7-b093-2dd6e78b5f6e",
    "target": "rac_demo_db"
}
```