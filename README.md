# Incremental Package

The Coalesce Base Node Types Package includes:

* [Incremental load](#incremental-load)
* [Looped Load](#looped-load)
* [Run view](#run-view)
* [Code](#code)

## Incremental Load

The Coalesce Incremental load node is a versatile node that allows you to develop and deploy a Stage table/view in Snowflake where we can perform incremental load in comparison with a persistent table added on top of it.

## Node Configuration

The Incremental node type has two configuration groups:

* [Node Properties](#node-properties)
* [Options](#options)

![Fact_config](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/f93d4d0f-f958-45ef-a471-a19b76d9c654)

### Node Properties

There are four configs within the **Node Properties** group.

* **Storage Location**: Storage Location where the WORK will be created.
* **Node Type**: Name of template used to create node objects.
* **Description**: A description of the node's purpose.
* **Deploy Enabled**:
  * If TRUE the node will be deployed / redeployed when changes are detected.
  * If FALSE the node will not be deployed or will be dropped during redeployment.

### Options

There are many configs within the **Options** group.

![Incremental_package_options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/e80cd292-8c52-436f-9a85-8c0e98a443aa)

* **Create As**:Provides option to choose materialization type as ‘table’ or ‘view’
* **Filter data based on Persistent table**: True / False toggle that helps us to load incremental load
  * True - provides option to perform incremnetal load.
  * False - a normal initial load of data from source is done.
* **Persistent table location**: The coalesce storage location is required to be added here.
* **Persistent table name**: The table name of the persistent table is required to be added here
* **Incremental load column(date)**: A date column based on which incremental data is loaded

## Example workflow

* Add a source node
* Add the Incremental UDN
* Leave the 'Filter data based on Persistent Table' option set too false
* Create
* Add the Persistent table to the view
* Create / Run
* Now go back to the Incremental UDN and change the 'Filter data based on Persistent Table' option to true.
* Use the pattern based option to match the persistent table (alter the definition of the UDN, if necessary), or add the table name manually in the last config item.
* Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including sub-select.
* Re-run the Incremental UDN

## Deployment

### Initial Deployment

When deployed for the first time into an environment the Incremental load node of materialization type table will execute the Create State Table.

**Create Stage Table**
This will execute a CREATE OR REPLACE statement and create a table in the target environment.

When deployed for the first time into an environment the Work node of materialization type view will execute the Create Stage View.

**Create Stage View**
This will execute a CREATE OR REPLACE statement and create a view in the target environment.

### Redeployment

After the Incremental Load  has been deployed for the first time into a target environment, subsequent deployments with column level changes or table level changes may result in altering the target Table.

#### Altering the Stage tables

There are few column or table changes like Change in table name,Dropping existing column, Alter Column data type,Adding a new column if made in isolation or all-together will result in an ALTER statement to modify the Work Table in the target environment.

The following stages are executed

* **Clone Table** - Creates an internal table
* **Rename Table | Alter Column| Delete Column | Add Column | Edit table description|**: Alter table statement is executed to perform the alter operation accordingly
* **Swap cloned Table** - Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost.
* **Delete Table** - Drops the internal table.

#### Recreating the Stage views

The subsequent deployment of Incremental load node of materialization type view with changes in view definition,adding table description or renaming view results in deleting the existing view and recreating the view

The following stages are executed:

* **Delete View**
* **Create View**

### Undeployment

If a Incremental load node of materialization type table is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the stage table in the target environment will be dropped.

This is executed in two stages:

* **Delete Table**: Coalesce Internal table is dropped.
* **Delete Table**: Target table in Snowflake is dropped.

If a Incremental load node of materialization type view is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the StageView in the target environment will be dropped.

The stage executed:

* **Delete View**: Drops the existing stage view from target environment.

## Looped Load

The Coalesce node Looped Load dynamically groups incoming source data, incrementally if configured, and loads into target data by looping through those groups.
It creates  “load buckets” dynamically based on a selection of table keys and then use those buckets to loop through source data and load into a target.

### Node Configuration

The Looped Load node type has two configuration groups:

* [Node Properties](#node-properties)
* [Options](#options)

![Fact_config](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/14825735-4a16-41ca-9964-7c785e8ab807)

### Node Properties

There are four configs within the **Node Properties** group.

* **Storage Location**: Storage Location where the WORK will be created.
* **Node Type**: Name of template used to create node objects.
* **Description**: A description of the node's purpose.
* **Deploy Enabled**:
  * If TRUE the node will be deployed / redeployed when changes are detected.
  * If FALSE the node will not be deployed or will be dropped during redeployment.

### Options

![Looped Load-options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/7df81cee-6ab8-478b-a60c-3c625cd2c927)

* **Enable tests**:This toggle can be enabled to perform some data quality tests.
* **Pre-SQL**: Any SQL to be executed as a predecessor to data insert operation can be mentioned here.
* **Post-SQL**: Any SQL to be executed post the data insert operation can be specified here.

### Load Options

![Incremental_load_options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/59f316de-c1a8-4e08-a028-dd31bfa4e092)

* **Load Type**:This toggle helps you to choose different accepted Load Types from Config.If no load type pararmeter is specified in the workspace,then it falls back to the config level load type.

### Record Selection Options

![LL-RecordSelectionOptions](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/7532bb77-b9da-4cf1-9d46-6334c020da44)

* **Grouping Type**:
  * Key Based:The incoming source data is grouped based on the bucket column as the key.
  * Range Buckets:The incoming source data is grouped based on the bucket column into different  groups mentioned in 'Number of groups' textbox.
    * Number of groups :The number of groups based on which data will be divided.
* **Bucket Columns**:The column based on data grouping is perfomed
* **Sort Group Data on Load**: True/False toggle that determines whether to add GROUP BY ALL to SQL Query.
  * True
    * Sort Colums:The Column name and sort order are prompted.
  * False

### Incremental load Options

![LL-Incremental-options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/d7fb8e69-f54d-4a7f-9247-6b31333adf59)

* **Set Incremental Load Options**:
  * True:Prompts for load column based on which incremental data is loaded
  * False:Incremental processing is not done
* **Incremental Load Column(date)**: A date column based on which incremental data is loaded
* **Group Incremental**: True/False toggle that determines whether to group data into single group or groups specified in the 'number of buckets' textbox.
  * True:The data is grouped on the number of buckets inputed
  * False:The data is grouped into a single group

### Group Table Options

![LL-Group table options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/9f5a1a25-a8f8-44ec-a408-9be5f9c01ee7)

* **Dedicated Load History Table**:
  * True:A history table specific to this target table is created by prefixing target table name to TABLE_GROUP_LOAD
  * False:A history table with common name 'TABLE_GROUP_LOAD' is created.
* **Load History Table Location**:
  * Same as Target:The history table is created in the same location as Target table.
  * Utility Schema: The history table is created in the location specified in   'UtilitySchema' parameter.

## Parameters

The Looped Load node specifies a loadType parameter to perform a full load ,a full reload,incremental load or reprocess load.
Also a parameter 'utilitySchema' can be used to set a target table in a location other than target table location.

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

## Key Points on Looped Load Node

* No data gets loaded when Load_type is set to 'Reprocess load' if the previous run was all successful.
  Only when the previous runs has any entries with process status '-1' denoting failure, data gets loaded during 'Reprocess Load'.
* The choice of bucket column is crucial for Keybased grouping as choosing a column with distinct unique values affects the performance of data load.

## Deployment

### Initial Deployment

When deployed for the first time into an environment the Looped load node will execute the below stages:

**Create Load Group Table**: This will execute a CREATE OR REPLACE statement and create a load group table in the target environment.

**Create Stage Table**: This will execute a CREATE OR REPLACE statement and create a table in the target environment.

### Redeployment

After the Looped Load  has been deployed for the first time into a target environment, subsequent deployments with column level changes or table level changes may result in altering the target Table.

#### Altering the target tables

There are few column or table changes like Change in table name,Dropping existing column, Alter Column data type,Adding a new column if made in isolation or all-together will result in an ALTER statement to modify the Work Table in the target environment.

The following stages are executed:

* **Clone Table**: Creates an internal table
* **Rename Table | Alter Column| Delete Column | Add Column | Edit table description|** : Alter table statement is executed to perform the alter operation accordingly;
* **Swap cloned Table**: Upon successful completion of all updates, the clone replaces the main table ensuring that no data is lost.
* **Delete Table**: Drops the internal table

### Undeployment

If a Incremental load node of materialization type table is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the stage table in the target environment will be dropped.

This is executed in two stages:

* **Delete Table**: Coalesce Internal table is dropped.
* **Delete Table**: Target table in Snowflake is dropped.

If a Incremental load node of materialization type view is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the StageView in the target environment will be dropped.

The stage executed:

* **Delete View**: Drops the existing stage view from target environment.

## Run View

The Coalesce node Run View is a variant of basic View node type.In contrast to standard View node,the custom node Run View actually includes the view created logic in the Run tab.  When a job using these views executes the view will be recreated as part of the job and will utilize whatever parameter values are passed in.
The parameters are used for selecting which sources to load into the target table.

### Node Configuration

The Run View node type has two configuration groups:

* [Node Properties](#node-properties)
* [Options](#options)

![Fact_config](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/932292e4-d843-4acf-80f4-f2fe5185f635)

### Node Properties

There are four configs within the **Node Properties** group.

* **Storage Location**: Storage Location where the WORK will be created.
* **Node Type**: Name of template used to create node objects.
* **Description**: A description of the node's purpose.
* **Deploy Enabled**:
  * If TRUE the node will be deployed / redeployed when changes are detected.
  * If FALSE the node will not be deployed or will be dropped during redeployment.

### Options

![Run View-options](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/a5e3b612-fbc7-480d-859d-d152527c0efb)

* **Multi Source**: True / False toggle that is Coalesce implementation of SQL UNIONs.
  * True - Multiple sources can be combined in a single node. The sources are combined using the option specified in the Multi Source Strategy.
  * False - Single source node or multiple sources combined using a join.
* **Override Create SQL**:True/False that determines whether a customized Create SQL is required to be executed.
  * True-Customized Create SQL specified in the Create SQL space is executed.All other options are invisible.
  * False-Create view SQL based on the options chosen are framed and executed.
* **Filter data based on source**:True/False that determines whether data is to be filtered based on parameter
  * True-Length of [source suffix](#tips-on-choosing-the-source-suffix) is required to be added to frame the filter in join clause.A point to remember here is clear the join tab and use the 'Copy To Editor' to add the new join after enabling 'Filter data based on source' option
  * False-No filter is added to the join tab.

### Increment Options

![Run View-increment](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/d382f5ec-80a3-4a11-bf05-c98df56a5e31)

* **Incremental load based on Persistent table**: True / False toggle that helps us to load incremental load
  * True - provides option to perform incremnetal load
  * False - a normal initial load of data from source is done
* **Persistent table location**: The coalesce storage location is required to be added here.
* **Persistent table name**: The table name of the persistent table is required to be added here
* **Incremental load column(date)**: A date column based on which incremental data is loaded

## Example Workflow

![Workflow](https://github.com/coalesceio/Incremental-Nodes/assets/7216836/84205415-3eb2-4d3a-a4ba-a3f21c4edb9f)

* Add a source node
* Add the Run View nodes on different sources
* Leave the 'Filter data based on Source' option set too false
* Create the base views from source
* Add another view node with multi source enabled to combine the sources created
* Enable the multi-source toggle and 'Filter data based on Source'
* Enter the length of the source suffix to identify the source.For instance if the source names follow the pattern RV_CUSTOMERA,RV_CUSTOMERB ,then the length of suffix can be set to 1 or if the source names follow thw pattern RV_COURSES_IT,RV_COURSES_SC,then the length of suffix can be set to 2 to uniquely identify the source to be loaded.
* Ensure the sourcesuffix for sources follow unique pattern like an alphabetical sequence(A,B,C,D) ,numerical sequence(1,2,34) or any meaningful pattern('SC','IT')
* Set the [parameter](#run-view-parameters) sourcesuffix to 'ALL' if you want to process all sources else set in accordance with the preferred sources to be processed
* Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including the filter clause for filtering out data from specific sources.
* Create / Run View
* You can dynamically create a view with data from preferred sources in case of multiple sources
  
### Incremental processing

* Add the Persistent table to the view
* Create / Run
* Now go back to the Run View node and set the Incremental options in Config.
* Remove the existing (basic) join and use the 'Copy To Editor' to add the new join, including sub-select.
* Re-run the Run View node

## Tips on choosing the source suffix

* Ensure that the multiple sources have the source name suffixes with a uniform pattern and length
* For example,the multiple sources can be named PRODUCTA,PRODUCTB,PRODUCTC and so on.In which case we can set the parameter **sourcesuffix** to ('A') to process only source PRODUCTA, ('A','B') to process sources PRODUCTA,PRODUCTB and ('ALL') to load all sources.In similar way we can have numerical suffixes as well.
  In this case the source suffix length in Config is to be set to **1**
* ANother example,source suffixes can have a meaningful pattern with uniform length like PRODUCT_ELEC,PRODUCT_HOME,PRODUCT_SPTS wherein the parameter **sourcesuffix** can be ('ELEC'),('HOME'),('ALL').In this case the source suffix length in Config is to be set to **4**

## Run View Parameters

The Run View node specifies a sourcesuffix parameter for selecting which sources to load into the target table.The sourcesuffix refers to the suffix of the sources you want to load to the target view.

For instance,if we have multiple sources like RV_CUSTOMERA,RV_CUSTOMERB,RV_CUSTOMERC.

* You can set the sourcesuffix to 'A' if you want to add data from only source RV_CUSTOMERA to target data.

  ```json
  {
    "sourcesuffix": "('A')"
   }
   ```

* You can set the sourcesuffix to 'A','B' if you want to add data from only source RV_CUSTOMERA,RV_CUSTOMERB to target data.

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

## Deployment

When deploying the DAG using Run View nodes the environment parameter should be set to ‘DEPLOY’.

**Deployment Value - When deploying this pipeline, use this parameter value**

   ```json
   {
    "sourcesuffix": "('DEPLOY')"
    }
   ```

When running a job with this pipeline, fill in the parameter with the sources you want to execute.

### Initial Deployment

When deployed for the first time into an environment the Run View will execute the below stages:

**Create View**
This will execute a CREATE OR REPLACE statement and create a view in the target environment.

### Redeployment

The subsequent deployment of View node with changes in view definition,adding table description,adding secure option or renaming view results in recreating the view

#### Recreating the view

The following stages are executed

**Create View**

### Undeployment

If a View Node is deleted from a Workspace, that Workspace is committed to Git and that commit deployed to a higher level environment then the View in the target environment will be dropped.

This is executed in the below stage:

**Delete View**
  
## Code

## Incremental load
* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/IncrementalLoad-230/run.sql.j2)

## Looped Load
* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/LoopedLoad-278/run.sql.j2)

## Run View
* [Node definition](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/definition.yml)
* [Create Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/create.sql.j2)
* [Run Template](https://github.com/coalesceio/Incremental-Nodes/blob/main/nodeTypes/RunView-281/run.sql.j2)

## Macros
[Macros](https://github.com/coalesceio/Incremental-Nodes/blob/main/macros/macro-1.yml)

[<img src="https://github.com/coalesceio/Incremental-Nodes/assets/7216836/140f6d7f-c0fc-4c28-b983-638787488391" alt="Git Logo" height="40">](https://github.com/coalesceio/Incremental-Nodes)
