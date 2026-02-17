# Incremental Package

The Coalesce Incremental Package includes:

* [Incremental load](#incremental-load)
* [Test Passed Records](#test-passed-records)
* [Test Failed Records](#test-failed-records)
* [Looped Load](#loop-load)
* [Run view](#run-view)
* [Grouped Incremental load](#grouped-incremental-load)
* [Code](#code)

## Incremental Load

The Coalesce Incremental load node is a versatile node that allows you to develop and deploy a Stage table/view in Snowflake where we can perform incremental load in comparison with a persistent table added on top of it.

### Incremental Load Node Configuration

* [Node Properties](#incremental-load-node-properties)
* [Options](#incremental-load-options)

#### Incremental Load Node Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the Incremental node will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Incremental Load Options

| **Options** | **Description** |
|-----------|-----------------|
| **Create As** | Provides option to choose materialization type as `table` or `view`. |
| **Filter data based on Persistent table** | - True - provides option to perform incremental load.<br/>- False - a normal initial load of data from source is done. |
| **Persistent table location(required)** | The Coalesce storage location. |
| **Persistent table name(required)** | The table name of the persistent table. |
| **Incremental load column(date)** | A date column based on which incremental data is loaded. |
| **Handle Data Quality**|When enabled you can add new columns to handle nullability and duplicate test.|
| **Nullanility test**| When enabled,you can choose columns based on which nullability checks needs to be done|
| **Duplicate test**|When enabled,you can choose columns based on which duplicate checks can be done|
| **Pre-SQL**|SQL to execute before data insert operation|
| **Post-SQL**|SQL to execute after data insert operation|

## Incremental Load Example Workflow

1. Add a source node.
2. Add the Incremental UDN.
3. Leave the 'Filter data based on Persistent Table' option set to False.
4. Create the node.
5. Add the Persistent table to the view.
6. Create and Run the node.
7. Go to the Incremental UDN and change the 'Filter data based on Persistent Table' option to true.
8. Use the pattern based option to match the persistent table (alter the definition of the UDN, if necessary), or add the table name manually in the last config item.
9. Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including sub-select.
10. Re-run the Incremental UDN.

## Incremental Load-With data quality Example Workflow

1. Add a source node.
2. Add the Incremental UDN.
3. Enable 'Handle Data Quality' toggle
4. When data quality is handled ,filter based on target(incremental processing)is diabled
5. Two types of Data Quality checks possible,Nullability and Duplicate records.
6. Lets assume the column name chosen for handling DQ checks is "ACCOUNT_ID".Then,you can find ACCOUNT_ID_nulll,ACCOUNT_ID_dup,TOTAL_FAILED_TESTS and QUALITY_FLAG added to target table
7. Re-sync the columns after table/view is created
8.The target table has data quality info with the flag set to 'G' or 'B' denoting good or bad records
9. Further,you can drill down good or bad records using [Test Passed records](#test-passed-records) or [Test failed records](#test-failed-records)

### Incremental Load Deployment

#### Incremental Load Initial Deployment

When deployed for the first time into an environment the Incremental load node of materialization type table will execute the Create State Table.

| **Stage** | **Description** |
|----------|-----------------|
| **Create Stage Table** | This will execute a CREATE OR REPLACE statement and create a table in the target environment. When deployed for the first time into an environment the Work node of materialization type view will execute the Create Stage View. |
| **Create Stage View** | This will execute a CREATE OR REPLACE statement and create a view in the target environment. |

#### Incremental Load Redeployment

After the Incremental Load  has been deployed for the first time into a target environment, subsequent deployments with column level changes or table level changes may result in altering the target Table.

#### Incremental Load Altering the Stage Tables

There are few column or table changes if made in isolation or all-together will result in an ALTER statement to modify the Work Table in the target environment.

* Changing the table name
* Dropping an existing column
* Altering Column data type
* Adding a new column

The following stages are executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Clone Table** | Creates an internal table. |
| **Rename Table \| Alter Column \| Delete Column \| Add Column \| Edit table description** | Alter table statement is executed to perform the alter operation. |
| **Swap cloned Table** | Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost. |
| **Delete Table** | Drops the internal table. |

#### Incremental Load Recreating the Stage Views

The subsequent deployment of Incremental load node of materialization type view with changes in view definition,adding table description or renaming view results in deleting the existing view and recreating the view.

The following stages are executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete View** | Delete the view |
| **Create View**| Create a new view |

### Incremental Load Undeployment

If a Incremental load node of materialization type table is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the stage table in the target environment will be dropped.

This is executed in two stages:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete Table** | Coalesce Internal table is dropped. |
| **Delete Table** | Target table in Snowflake is dropped. |

If a Incremental load node of materialization type view is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the StageView in the target environment will be dropped.

The stage executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete View** | Drops the existing stage view from target environment. |

## Test Passed records

The Coalesce "Test Passed Records" (GR) node is a specialized filtering node type designed to extract "clean" data from a source that has already undergone Data Quality (DQ) processing in Incremental node. It serves as a gatekeeper, ensuring only records that meet your business standards move further downstream.

## Key-functions

* **Quality Filtering:** By default, it acts as a simple filter that only selects records where the QUALITY_FLAG is set to 'G' (Good/Passed).
* **Duplicate Salvaging:** Unlike a standard filter, it provides logic to "salvage" duplicated records. If a record is flagged as a duplicate, you can configure the node to keep either the First Occurrence or the Last Occurrence based on a chosen timestamp column (timcol).
* **Always use on an incremental node with data quality handled**

## Test passed records Example Workflow

1. Add a source node.
2. Add the Incremental UDN.
3. By default,where clause to filter 'G' quality is added in JOIN tab
4. If you want the first or last occurrence of record as valid enable corresponding toggles in config
5. Go to join tab and override join clause by clicking on 'Copy to editor'
6. The target will have good quality records

### Test Passed records Load Node Configuration

* [Node Properties](#test-passed-records-properties)
* [Options](#test-passed-records-options)

#### Test Passed records Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the test passed records node will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Test Passed records Options

| **Options** | **Description** |
|-----------|-----------------|
| **Create As** | Provides option to choose materialization type as `table` or `view`. |
| **First Occurence of Duplicate considered as valid**|When enabled,timestamp column should be provided to add the first occurence of duplicate as valid record based on timestamp column|
| **Last Occurence of Duplicate considered as valid**|When enabled,timestamp column should be provided to add the last occurence of duplicate as valid record based on timestamp column|

## Test Failed records

The Coalesce "Test Failed Records" node acts as a Quarantine and Error-Capture component. This node isolates all records that failed your Data Quality (DQ) tests for auditing and remediation.

## Key-functions

* **Error Isolation:** It filters the source to extract only records where the QUALITY_FLAG is 'B', ensuring that any record failing your Data Quality (DQ) standards is captured for auditing. This ensures that bad quality data is kept separate from your production analytics.
* **2.Duplicate Capture:** If deduplication logic is enabled (First or Last Occurrence), this node specifically captures the rejected duplicates, and remediation.

* **Always use on an incremental node with data quality handled**
  
## Test failed records Example Workflow

1. Add a source node.
2. Add the Incremental UDN.
3. By default,where clause to filter 'B' quality is added in JOIN tab
4. If you want the first or last occurrence of record as valid enable corresponding toggles in config
5. Go to join tab and override join clause by clicking on 'Copy to editor'
6. The target will have good quality records

### Test Failed records Load Node Configuration

* [Node Properties](#test-failed-records-properties)
* [Options](#test-failed-records-options)

#### Test Failed records Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the test passed records node will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Test Failed records Options

| **Options** | **Description** |
|-----------|-----------------|
| **Create As** | Provides option to choose materialization type as `table` or `view`. |
| **First Occurence of Duplicate considered as valid**|When enabled,timestamp column should be provided to capture the records other than first occurence of duplicate considered as  valid record based on timestamp column as bad quality records|
| **Last Occurence of Duplicate considered as valid**|When enabled,timestamp column should be provided to capture the records other than last occurence of duplicate considered as  valid record based on timestamp column as bad quality record|

## Loop Load

The Coalesce node Looped Load dynamically groups incoming source data (incrementally if configured) and loads it into target data by looping through those groups.

It creates "load buckets" dynamically based on a selection of table keys and then uses those buckets to loop through source data and load into a target.

### Looped Load Node Configuration

* [Looped Load Node Properties](#looped-load-node-properties)
* [Looped Load Options](#looped-load-options)
* [Looped Load Incremental Load Options](#looped-load-incremental-load-options)
* [Looped Load Group Table Options](#looped-load-group-table-options)

#### Looped Load Node Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the WORK will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Looped Load Options

| **Option** | **Description** |
|----------|-----------------|
| **Enable tests** | This toggle can be enabled to perform data quality tests. |
| **Pre-SQL** | Any SQL to be executed as a predecessor to the data insert operation can be specified here. |
| **Post-SQL** | Any SQL to be executed after the data insert operation can be specified here. |
| **Load Type** | This toggle helps you choose different accepted Load Types from Config. If no load type parameter is specified in the workspace, then it falls back to the config level load type. |

#### Looped Load Incremental Load Options

| **Option** | **Description** |
|----------|-----------------|
| **Set Incremental Load Options** | - **True**: Prompts for load column based on which incremental data is loaded<br/>- **False**: Incremental processing is not done |
| **Incremental Load Column(date)** | A date column based on which incremental data is loaded |
| **Group Incremental** | - **True**: The data is grouped based on the number of buckets input<br/>- **False**: The data is grouped into a single group |

#### Looped Load Group Table Options

| **Option** | **Description** |
|----------|-----------------|
| **Dedicated Load History Table** | - **True**: A history table specific to this target table is created by prefixing target table name to `TABLE_GROUP_LOAD`<br/>- **False**: A history table with common name `TABLE_GROUP_LOAD` is created |
| **Load History Table Location** | - Same as Target: The history table is created in the same location as Target table<br/>- Utility Schema: The history table is created in the location specified in `UtilitySchema` parameter |

### Looped Load Parameters

The Looped Load node specifies a `loadType` parameter to perform:

* A full load
* A full reload
* Incremental load
* Reprocess load

Also a parameter `utilitySchema` can be used to set a target table in a location other than target table location.

```json
{
     "loadType_accepted_values": [
             "Incremental Load",
                    "Full Load",
               "Reprocess Load",
                  "Full Reload"
 ],
       "loadType": "Full Load",
   "utilitySchema": "TARGET_DB"
}
```

### Key Points on Looped Load Node

* No data gets loaded when `Load_type` is set to `Reprocess load` if the previous run was all successful.
  Only when the previous runs has any entries with process status '-1' denoting failure, data gets loaded during `Reprocess Load`.
* The choice of bucket column is crucial for Keybased grouping as choosing a column with distinct unique values affects the performance of data load.

### Looped Load Deployment

#### Looped Load Initial Deployment

When deployed for the first time into an environment the Looped load node will execute the following stages:

| **Stage** | **Description** |
|----------|-----------------|
| **Create Load Group Table** | This will execute a CREATE OR REPLACE statement and create a load group table in the target environment. |
| **Create Stage Table** | This will execute a CREATE OR REPLACE statement and create a table in the target environment. |

#### Looped Load Redeployment

After the Looped Load  has been deployed for the first time into a target environment, subsequent deployments with column level changes or table level changes may result in altering the target Table.

#### Looped Load Altering the Target Tables

Column or table changes that will result in an ALTER statement to modify the Work Table in the target environment. These changes can be made either in isolation or all together.

* Change in table name
* Dropping existing column
* Alter Column data type
* Adding a new column

The following stages are executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Clone Table** | Creates an internal table |
| **Rename Table \| Alter Column \| Delete Column \| Add Column \| Edit table description** | Alter table statement is executed to perform the alter operation accordingly |
| **Swap cloned Table** | Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost |
| **Delete Table** | Drops the internal table |

### Looped Load Undeployment

If a Incremental load node of materialization type table is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the stage table in the target environment will be dropped.

This is executed in two stages:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete Table** | Coalesce Internal table is dropped |
| **Delete Table** | Target table in Snowflake is dropped |

If a Incremental load node of materialization type view is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the StageView in the target environment will be dropped.

The stage executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete View** | Drops the existing stage view from target environment |

## Run View

The Coalesce Run View is a variant of the basic View node type. In contrast to the standard View node, the custom Run View node actually includes the view creation logic in the Run tab. When a job using these views executes, the view will be recreated as part of the job and will utilize whatever parameter values are passed in. The parameters are used for selecting which sources to load into the target table.

### Run View Node Configuration

* [Node Properties](#run-view-node-properties)
* [Run View Options](#run-view-options)
* [Run View Increment Options](#run-view-increment-options)

#### Run View Node Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the WORK will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Run View Options

| **Options** | **Description** |
|---|---|
| **Multi Source** | True / False toggle that is Coalesce implementation of SQL UNIONs. <br/> - **True** - Multiple sources can be combined in a single node. The sources are combined using the option specified in the Multi Source Strategy. <br/> - **False** - Single source node or multiple sources combined using a join. |
| **Override Create SQL** | True/False that determines whether a customized Create SQL is required to be executed. <br/> - **True** - Customized Create SQL specified in the Create SQL space is executed. All other options are invisible. <br/> - **False** - Create view SQL based on the options chosen are framed and executed. |
| **Filter data based on source** | True/False that determines whether data is to be filtered based on parameter <br/> - **True** - Length of [source suffix](#tips-on-choosing-the-source-suffix) is required to be added to frame the filter in join clause. A point to remember here is clear the join tab and use the 'Copy To Editor' to add the new join after enabling 'Filter data based on source' option <br/> - **False** - No filter is added to the join tab. |

#### Run View Increment Options

| **Options** | **Description** |
|---|---|
| **Incremental load based on Persistent table** | True / False toggle that helps us to load incremental load. <br/> - **True** - provides option to perform incremental load. <br/> - **False** - a normal initial load of data from source is done. |
| **Persistent table location (required)** | The Coalesce storage location is required to be added here. |
| **Persistent table name (required)** | The table name of the persistent table is required to be added. |
| **Incremental load column(date)** | A date column based on which incremental data is loaded. |

### Run View Example Workflow

![Workflow](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/84205415-3eb2-4d3a-a4ba-a3f21c4edb9f)

1. Add a source node.
2. Add the Run View nodes on different sources.
3. Leave the `Filter data based on Source` option set to false.
4. Create the base views from source.
5. Add another view node with multi source enabled to combine the created sources.
6. Enable the multi-source toggle and `Filter data based on Source`.
7. Enter the length of the source suffix to identify the source. For instance, if the source names follow the pattern `RV_CUSTOMERA`, `RV_CUSTOMERB`, then the length of suffix can be set to 1, or if the source names follow the pattern `RV_COURSES_IT`, `RV_COURSES_SC`, then the length of suffix can be set to 2 to uniquely identify the source to be loaded.
8. Ensure the source suffix for sources follows a unique pattern like an alphabetical sequence (A,B,C,D), numerical sequence (1,2,3,4) or any meaningful pattern ('SC','IT').
9. Set the [parameter](#run-view-parameters) source suffix to `ALL` if you want to process all sources, else set it in accordance with the preferred sources to be processed.
10. Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including the filter clause for filtering out data from specific sources.
11. Create and Run the Run View.
12. You can dynamically create a view with data from preferred sources in case of multiple sources.

### Run View Incremental Processing Example

1. Add the Persistent table to the view.
2. Create and Run the node.
3. Now go back to the Run View node and set the Incremental options in Config.
4. Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including sub-select.
5. Re-run the Run View node.

### Tips on Choosing the Source Suffix

* Ensure that the multiple sources have source name suffixes with a uniform pattern and length.
* For example, the multiple sources can be named PRODUCTA, PRODUCTB, PRODUCTC, and so on. In this case, we can set the parameter **sourcesuffix** to ('A') to process only source PRODUCTA, ('A','B') to process sources PRODUCTA, PRODUCTB, and ('ALL') to load all sources. In a similar way, we can have numerical suffixes as well. In this case, the source suffix length in Config is to be set to **1**.
* Another example, source suffixes can have a meaningful pattern with uniform length like *`PRODUCT_ELEC`*, *`PRODUCT_HOME`*, *`PRODUCT_SPTS`* wherein the parameter **sourcesuffix** can be ('ELEC'), ('HOME'), ('ALL'). In this case, the source suffix length in Config is to be set to **4**.

### Run View Parameters

The Run View node specifies a sourcesuffix parameter for selecting which sources to load into the target table.The sourcesuffix refers to the suffix of the sources you want to load to the target view.

For instance,if we have multiple sources like `RV_CUSTOMERA`, `RV_CUSTOMERB`, and `RV_CUSTOMERC`.

* You can set the sourcesuffix to 'A' if you want to add data from only source `RV_CUSTOMERA` to target data.

```json
{
  "sourcesuffix": "('A')"
  }
```

* You can set the sourcesuffix to 'A','B' if you want to add data from only source `RV_CUSTOMERA` and `RV_CUSTOMERB` to target data.

```json
  {
  "sourcesuffix": "('A','B')"
  }
```

* You can set  the sourcesuffix to 'ALL' if you want to add data from all sources to target data.
  
```json
{
  "sourcesuffix": "('ALL')"
  }
```

* You can add numeric prefixes.
  
```json
{
"sourcesuffix": "('1','2')"
}
```

### Run View Deployment

When deploying the DAG using Run View nodes the environment parameter should be set to `DEPLOY`.

**Deployment Value** - When deploying this pipeline, use this parameter value.

```json
{
"sourcesuffix": "('DEPLOY')"
}
```

When running a job with this pipeline, fill in the parameter with the sources you want to execute.

#### Run View Initial Deployment

When deployed for the first time into an environment the Run View will execute the following stages:

| **Stage** | **Description** |
|---|---|
| **Create View** | This will execute a CREATE OR REPLACE statement and create a view in the target environment. |

#### Run View Redeployment

The subsequent deployment of a View node with changes in view definition, adding table description, adding secure option, or renaming view results in recreating the view.

**Recreating the View**

| **Stage** | **Description** |
|---|---|
| **Create View** | This will execute a CREATE OR REPLACE statement and create a view in the target environment. |

### Run View Undeployment

If a View Node is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the View in the target environment will be dropped.

This is executed in the below stage:

* **Delete View**

## Grouped Incremental Load

The Coalesce Grouped Incremental load node is a versatile node that allows you to develop and deploy a Stage table/Transient Table in Snowflake where we can perform Grouped Incremental load from different storage locations with a persistent table added on top of it.

### Grouped Incremental Load Node Configuration

* [Node Properties](#grouped-incremental-load-node-properties)
* [Options](#grouped-incremental-load-options)

#### Grouped Incremental Load Node Properties

| **Property** | **Description** |
|-------------|-----------------|
| **Storage Location** | Storage Location where the WORK will be created. |
| **Node Type** | Name of template used to create node objects. |
| **Description** | A description of the node's purpose. |
| **Deploy Enabled** | - If TRUE the node will be deployed / redeployed when changes are detected.<br/>- If FALSE the node will not be deployed or will be dropped during redeployment. |

#### Grouped Incremental Load Options

| **Options** | **Description** |
|-----------|-----------------|
| **Create As** | Provides option to choose materialization type as `table` or `transient table`. |
| **Storage Mapping ID** | Provides option enter storage locations defined in Coalesce separated by comma(E.g EDW_RAW,EDW_PA). |
| **Batch Load** | - True - provides option to perform data loading in batches.<br/>- False - a normal load of data from each storage location is done. |
| **Persistent table location(required)** | The Coalesce storage location of Persistent table. |
| **Persistent table name(required)** | The table name of the persistent table. |
| **Persistent table Column Name(required)** | The column name of the persistent table which represents the storage location id. |
| **Joining Table Column Name(optional)** | The column name of the table which will be joined with main table. |
| **Grouped Incremental load column(date)** | A date column based on which grouped incremental data is loaded. |

## Grouped Incremental Load Example Workflow

1. Add a source node.
2. Add the Grouped Incremental UDN.
3. Add Storage Mapping ID seprated by comma.
3. Leave the 'Batch Load' option set to False.
4. Enter Persistent Table location
5. Enter Persistent Table name 
6. Enter Persistent Table Column Name 
7. Enter Joining Table Column Name if join is present
8. Select Incremental Load Column
9. Generate join, use the 'Copy To Editor' to add the new join, including sub-select..
10.Create and Run the node.


### Grouped Incremental Load Deployment

#### Grouped Incremental Load Initial Deployment

When deployed for the first time into an environment the Grouped Incremental load node of materialization type table will execute the Create State Table.

| **Stage** | **Description** |
|----------|-----------------|
| **Create Stage Table** | This will execute a CREATE OR REPLACE statement and create a table in the target environment. When deployed for the first time into an environment the Work node of materialization type view will execute the Create Stage View. |
| **Create Stage View** | This will execute a CREATE OR REPLACE statement and create a view in the target environment. |

#### Grouped Incremental Load Redeployment

After the Grouped Incremental Load  has been deployed for the first time into a target environment, subsequent deployments with column level changes or table level changes may result in altering the target Table.

#### Grouped Incremental Load Altering the Stage Tables

There are few column or table changes if made in isolation or all-together will result in an ALTER statement to modify the Work Table in the target environment.

* Changing the table name
* Dropping an existing column
* Altering Column data type
* Adding a new column

The following stages are executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Clone Table** | Creates an internal table. |
| **Rename Table \| Alter Column \| Delete Column \| Add Column \| Edit table description** | Alter table statement is executed to perform the alter operation. |
| **Swap cloned Table** | Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost. |
| **Delete Table** | Drops the internal table. |

#### Grouped Incremental Load Altering the Transient Tables

There are few column or table changes if made in isolation or all-together will result in an ALTER statement to modify the table in the target environment.

* Changing the table name
* Dropping an existing column
* Altering Column data type
* Adding a new column

The following stages are executed:

| **Stage** | **Description** |
|----------|-----------------|
| **Clone Table** | Creates an internal table. |
| **Rename Table \| Alter Column \| Delete Column \| Add Column \| Edit table description** | Alter table statement is executed to perform the alter operation. |
| **Swap cloned Table** | Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost. |
| **Delete Table** | Drops the internal table. |

### Redeployment with no changes 

If the nodes are redeployed with no changes compared to previous deployment,then no stages are executed

### Grouped Incremental Load Undeployment

If a Grouped Incremental load node of materialization type table is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the stage table in the target environment will be dropped.

This is executed in two stages:

| **Stage** | **Description** |
|----------|-----------------|
| **Delete Table** | Coalesce Internal table is dropped. |
| **Delete Table** | Target table in Snowflake is dropped. |

If a Grouped Incremental load node of materialization type view is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the StageView in the target environment will be dropped.


### Code

#### Incremental Load Code

* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/run.sql.j2)

#### Looped Load Code

* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/run.sql.j2)

#### Run View Code

* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/run.sql.j2)

#### Run View Macros

* [Macros](https://github.com/coalesceio/Incremental-Nodes/blob/main/macros/macro-1.yml)

#### Grouped Incremental Load Code

* [Node definition](https://github.com/coalesceio/Incremental-Nodes/tree/main/nodeTypes/GroupedIncrementalLoad-394/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/tree/main/nodeTypes/GroupedIncrementalLoad-394/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/tree/main/nodeTypes/GroupedIncrementalLoad-394/run.sql.j2)
