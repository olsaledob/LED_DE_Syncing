# About
LED_DE_Syncing is used for synchronizing timestamps of an Arduino to the digital events controlling it. The Arduino is not as stable and reliable as the digital events, therefore the latter is treated as the ground-truth. 

# Setup
## Creating the environment
You can set up the environment with **Conda/Miniforge** or **pip**.
### Option 1: Conda/Miniforge
```bash
conda env create -f environment.yml
conda activate syncenv
```
### Option 2: pip
```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Running the Sync
*Note: In the future new methods for selecting recordings will be added. For now this is limited to folder-handling.*

Once you have your environment set up and activated, you can run the sync script from the root of the repository. 

```bash
python run_sync.py
```

Make sure you configure the `config.toml` file in the project root. It should contain your paths and parameters. Most importantly, set the values below accordingly:


```toml
[paths]
path_led_dir     = "E:\\Git\\CIDBN\\SyncingExampleData\\LED"
path_h5_dir      = "E:\\Git\\CIDBN\\SyncingExampleData\\DE"
path_to_results  = "E:\\Git\\CIDBN\\SyncingExampleData\\Results"
log_dir          = "E:\\Git\\CIDBN\\SyncingExampleData\\Logs"

[parameters]
rec_id_start        = 1
rec_id_end          = 999
```

Depending on your operating system, make sure to pass the locations correctly (e.g. escape '\' under windows).

