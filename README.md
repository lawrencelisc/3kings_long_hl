# 3kings_long_hl


### Installation

---
1. Clone the repository

   - Using SSH
   ```shell script
   git clone xxx
   ``` 
   
   - Using HTTPS with Personal Access Token
   ```shell script
   git clone xxx
   ```

2. Set up the Virtual Environment

    Ubuntu 20.04 (Debian-based Linux)
    ```shell script
    cd ./algo_3kings_long_hl
    python3.12 -m venv venv/
    source ./venv/bin/activate
    ```
   
    Windows 10
    ```shell script
    cd .\algo_3kings_long_hl
    python -m venv .\venv\
    .\venv\Scripts\activate
    ```

3. Install the dependencies

    ```shell script
    pip install -r requirements.txt
    pip install --upgrade pip
    ```


### Deployment

---
#### Dev Environment
1. Run the application
    ```shell script
    python3.12 algo_3kings_long_hl.py
    ```

#### Running via Systemd
1. Move the file to Systemd's system folder.
    ```shell script
    sudo cp ./algo_3kings_long_hl.service /etc/systemd/system/algo_3kings_long_hl.service
    ```
2. Enable and start the service.
    ```shell script
    sudo systemctl daemon-reload
    sudo systemctl enable 3kings_long_hl.service
    sudo systemctl start 3kings_long_hl.service
    ```
3. Check if the application is running.
    ```shell script
    sudo systemctl status 3kings_long_hl.service
    ```
# 3kings_long_hl

# 3kings_long_hl
