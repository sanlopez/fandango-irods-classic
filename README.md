# FandanGO - iRODS plugin

This is the iRODS plugin of the FandanGO application.

## How to deploy it

1. You should have previously set up the core plugin.

2. You should have an iRODS zone properly set up. And you need to provide details about it in the ``config.yaml`` file at ``ZONE`` section.

3. Create a conda environment and install the needed packages:
   ```
   conda create --name fandango_irods_env python=3.12
   conda activate fandango_irods_env
   cd fandango-irods
   pip install -r requirements.txt
   ```

4. Database setup:

   - You should have MySQL installed. Since the core plugin is set up, you already have it. Make sure the service is running (``systemctl status mysql``)
   
   - Create **fandango_irods** database and grant the **fandango** access to the database. This could be done as follows from the MySQL shell:
     ```
     CREATE DATABASE fandango_irods;
     GRANT ALL PRIVILEGES ON fandango_irods.* TO 'fandango'@'localhost';
     FLUSH PRIVILEGES;
     ```
   
   - Edit the ``config.yaml`` file for setting the **fandango_user** password (``PASS`` attribute from ``DDBB`` section).
   
   - Create the needed tables by executing ``create_model.py`` script: 
     ```
     conda activate fandango_irods_env
     cd fandango-irods/db
     python create_model.py
     ```
     

## Functions currently implemented 

- **copyData**: creates an iRODS collection from data provided. Args: 
   - projectID [required]: project ID given by core's plugin
   - rawData [required]: path of the raw data 
   - writePermission [optional, default false]: should the iRODS collection have write permission? 

    Example/s of call from core's plugin:
    ```
    python main.py --action=copyData --plugin=irods --projectId=202401011 --rawData=/path/to/data --writePermission=yes
    python main.py --action=copyData --plugin=irods --projectId=202401011 --rawData=/path/to/data
    ```
