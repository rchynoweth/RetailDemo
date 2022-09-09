# Retail Demo 


### Features
Current: 
- Simulates, ingests, and processes transactional data 
- Process and fulfills orders 
- Notifies customers at multiple touchpoints 

Future: 
- Substitutions 
1. Pickup timeslots 
1. Basket creation (adding/removing items from cart). This could potentially be used to predict out of stock items. 
1. Tables to prioritize picking i.e. focus on the near term pick up orders 




<img src="https://racadlsgen2.blob.core.windows.net/public/RCGDemosDiagrams.png" width = 1200/>



### Resources:
- [Programmatically Manage and Create Multiple Live Tables](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cookbook.html#programmatically-manage-and-create-multiple-live-tables)


### DLT Configuration
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